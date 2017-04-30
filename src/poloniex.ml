open Core
open Async

open Bs_devkit
open Bs_api.PLNX

module Encoding = Dtc_pb.Encoding
module DTC = Dtc_pb.Dtcprotocol_piqi

let rec loop_log_errors ?log f =
  let rec inner () =
    Monitor.try_with_or_error ~name:"loop_log_errors" f >>= function
    | Ok _ -> assert false
    | Error err ->
      Option.iter log ~f:(fun log -> Log.error log "run: %s" @@ Error.to_string_hum err);
      inner ()
  in inner ()

let conduit_server ~tls ~crt_path ~key_path =
  if tls then
    Sys.file_exists crt_path >>= fun crt_exists ->
    Sys.file_exists key_path >>| fun key_exists ->
    match crt_exists, key_exists with
    | `Yes, `Yes -> `OpenSSL (`Crt_file_path crt_path, `Key_file_path key_path)
    | _ -> failwith "TLS crt/key file not found"
  else
  return `TCP

let exchange = "PLNX"
let plnx_historical = ref ""
let exchange_account = "exchange"
let margin_account = "margin"
let update_client_span = ref @@ Time_ns.Span.of_int_sec 10

let log_plnx =
  Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_dtc =
  Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]

module Book = struct
  type t = {
    mutable bid: Int.t Int.Map.t ;
    mutable ask: Int.t Int.Map.t ;
  }

  let create () = {
    bid = Int.Map.empty ;
    ask = Int.Map.empty ;
  }
end

let reqid_to_sym : String.t Int.Table.t = Int.Table.create ()
let subid_to_sym : String.t Int.Table.t = Int.Table.create ()
let sym_to_subid : Int.t String.Table.t = String.Table.create ()

let currencies : Rest.currency String.Table.t = String.Table.create ()
let tickers : (Time_ns.t * ticker) String.Table.t = String.Table.create ()

let buf_json = Bi_outbuf.create 4096
let buf_cs = Bigstring.create 4096

let failure_of_error e =
  match Error.to_exn e |> Monitor.extract_exn with
  | Failure msg -> Some msg
  | _ -> None

let descr_of_symbol s =
  let buf = Buffer.create 32 in
  match String.split s ~on:'_' with
  | [quote; base] ->
    let quote = String.Table.find_exn currencies quote in
    let base = String.Table.find_exn currencies base in
    Buffer.add_string buf base.name;
    Buffer.add_string buf " / ";
    Buffer.add_string buf quote.name;
    Buffer.contents buf
  | _ -> invalid_argf "descr_of_symbol: %s" s ()

let secdef_of_ticker ?(request_id=110_000_000l) ?(final=true) t =
  let secdef = DTC.default_security_definition_response () in
  secdef.request_id <- Some request_id ;
  secdef.is_final_message <- Some final ;
  secdef.symbol <- Some t.symbol ;
  secdef.exchange <- Some exchange ;
  secdef.security_type <- Some `security_type_forex ;
  secdef.description <- Some (descr_of_symbol t.symbol) ;
  secdef.min_price_increment <- Some 1e-8 ;
  secdef.currency_value_per_increment <- Some 1e-8 ;
  secdef.price_display_format <- Some `price_display_format_decimal_8 ;
  secdef.has_market_depth_data <- Some true ;
  secdef

let books : Book.t String.Table.t = String.Table.create ()
let latest_trades : DB.trade String.Table.t = String.Table.create ()

module TradeHistory = struct
  module T = struct
    type t = Rest.trade_history [@@deriving sexp]
    let compare t t' = Int.compare t.Rest.id t'.Rest.id
  end
  include T
  module Set = Set.Make(T)
end

module OpenOrders = struct
  module T = struct
    type t = Rest.open_orders_resp [@@deriving sexp]
    let compare (t:t) (t':t) = Int.compare t.Rest.id t'.Rest.id
  end
  include T
  module Set = Set.Make(T)
end

module MarginPosition = struct
  module T = struct
    type t = { symbol: string; p: Rest.margin_position } [@@deriving sexp]
    let compare (t:t) (t':t) = String.compare t.symbol t'.symbol
  end
  include T
  module Set = Set.Make(T)
end

module Connection = struct
  type t = {
    addr: Socket.Address.Inet.t;
    addr_str: string;
    w: Writer.t;
    key: string;
    secret: Cstruct.t;
    mutable dropped: int;
    subs: int String.Table.t;
    subs_depth: int String.Table.t;
    (* Balances *)
    b_exchange: Rest.balance String.Table.t;
    b_margin: Int.t String.Table.t;
    mutable margin: Rest.margin_account_summary;
    (* Orders & Trades *)
    orders: (string * Rest.open_orders_resp) Int.Table.t;
    trades: TradeHistory.Set.t String.Table.t;
    mutable positions: MarginPosition.Set.t;
    send_secdefs : bool ;
  }

  module Table = InetAddr.Table

  let active : t Table.t = Table.create ()

  let update_positions ({ addr_str; w; key; secret; positions } as c) =
    let update_msg ?(price=0) ?(qty=0) symbol =
      let update = DTC.default_position_update () in
      update.trade_account <- Some margin_account ;
      update.total_number_messages <- Some 1l ;
      update.message_number <- Some 1l ;
      update.symbol <- Some symbol ;
      update.exchange <- Some exchange ;
      update.quantity <- Some (qty // 100_000_000) ;
      update.average_price <- Some (price // 100_000_000) ;
      update in
    Rest.margin_positions ~buf:buf_json ~key ~secret () >>| function
    | Error err ->
      Log.error log_plnx "update positions (%s): %s"
        addr_str @@ Rest.Http_error.to_string err
    | Ok ps ->
      let cur_pos = MarginPosition.Set.of_list @@
        List.filter_map ps ~f:begin function
        | _, None -> None
        | symbol, Some p -> Some { MarginPosition.symbol; p }
        end in
      let new_pos = MarginPosition.Set.diff cur_pos positions in
      let old_pos = MarginPosition.Set.diff positions cur_pos in
      c.positions <- cur_pos;
      MarginPosition.Set.iter old_pos ~f:(fun { symbol } -> write_update symbol);
      MarginPosition.Set.iter new_pos ~f:begin fun { symbol; p={ price; qty } } ->
        write_update ~price ~qty symbol
      end

  let update_orders { addr_str; key; secret; orders } =
    Rest.open_orders ~buf:buf_json ~key ~secret () >>|
    Result.map ~f:begin fun os ->
      Int.Table.clear orders;
      List.iter os ~f:begin fun (symbol, os) ->
        List.iter os ~f:begin fun (o:Rest.open_orders_resp) ->
          Int.Table.set orders o.id (symbol, o)
        end
      end
    end

  let update_trades ({ addr_str; w; key; secret; trades } as conn) =
    Rest.trade_history ~buf:buf_json ~key ~secret () >>= function
    | Error err ->
      return @@ Log.error log_plnx "update trades (%s): %s"
        addr_str @@ Rest.Http_error.to_string err
    | Ok ts ->
      update_positions conn >>= fun () ->
      Clock_ns.after Time_ns.Span.(of_int_ms 50) >>= fun () ->
      update_orders conn >>| function
      | Error err ->
        Log.error log_plnx "update orders (%s): %s"
          addr_str @@ Rest.Http_error.to_string err
      | Ok () ->
        List.iter ts ~f:begin fun (symbol, ts) ->
          let old_ts =
            String.Table.find trades symbol |>
            Option.value ~default:TradeHistory.Set.empty in
          let cur_ts = TradeHistory.Set.of_list ts in
          let new_ts = TradeHistory.Set.diff cur_ts old_ts in
          String.Table.set trades symbol cur_ts;
          TradeHistory.Set.iter new_ts ~f:ignore (* TODO: send order update messages *)
        end

  let update_trades_loop ?start conn span =
    Clock_ns.every
      ?start ~stop:(Writer.close_started conn.w) ~continue_on_error:true span
      (fun () -> don't_wait_for @@ update_trades conn)

  let write_margin_balance
      ?request_id
      ?(nb_msgs=1)
      ?(msg_number=1) { addr_str; w; b_margin; margin } =
    let update_cs =
      Cstruct.of_bigarray ~off:0 ~len:Account.Balance.Update.sizeof_cs buf_cs in
    let currency = "mBTC" in
    let cash_balance = margin.total_value // 100_000 in
    let securities_value = margin.net_value // 100_000 in
    let margin_requirement = margin.total_borrowed_value // 100_000 *. 0.2 in
    let balance_available =
      securities_value /. 0.4 -. margin.total_borrowed_value // 100_000 in
    Account.Balance.Update.write
      ?request_id ~nb_msgs ~msg_number ~trade_account:margin_account
      ~currency ~cash_balance ~balance_available
      ~securities_value ~margin_requirement update_cs;
    Writer.write_cstruct w update_cs;
    Log.debug log_dtc "-> %s AccountBalanceUpdate %s (%d/%d)"
      addr_str margin_account msg_number nb_msgs

  let write_exchange_balance
      ?request_id
      ?(nb_msgs=1)
      ?(msg_number=1) { addr_str; w; b_exchange } =
    let update_cs =
      Cstruct.of_bigarray ~off:0 ~len:Account.Balance.Update.sizeof_cs buf_cs in
    let currency = "mBTC" in
    let b = String.Table.find b_exchange "BTC" |>
            Option.map ~f:begin fun { Rest.available; on_orders } ->
              available // 100_000, (available - on_orders) // 100_000
            end
    in
    let cash_balance = Option.map b ~f:fst in
    let balance_available = Option.map b ~f:snd in
    let securities_value =
      String.Table.fold b_exchange ~init:0
        ~f:begin fun ~key:_ ~data:{ Rest.btc_value } a ->
          a + btc_value end // 100_000
    in
    let margin_requirement = 0. in
    Account.Balance.Update.write
      ?request_id ~nb_msgs ~msg_number
      ~trade_account:exchange_account
      ~currency ?cash_balance ?balance_available
      ~securities_value ~margin_requirement update_cs;
    Writer.write_cstruct w update_cs;
    Log.debug log_dtc "-> %s AccountBalanceUpdate %s (%d/%d)"
      addr_str exchange_account msg_number nb_msgs

  let update_balances ({ addr_str; w; key; secret; b_exchange; b_margin } as conn) =
    Rest.margin_account_summary ~buf:buf_json ~key ~secret () >>| begin function
    | Error err -> Log.error log_plnx "%s" @@ Rest.Http_error.to_string err
    | Ok m -> conn.margin <- m
    end >>= fun () ->
    Clock_ns.after Time_ns.Span.(of_int_ms 50) >>= fun () ->
    Rest.positive_balances ~buf:buf_json ~key ~secret () >>| begin function
    | Error err -> Log.error log_plnx "%s" @@ Rest.Http_error.to_string err
    | Ok bs ->
      String.Table.clear b_margin;
      List.Assoc.find ~equal:(=) bs Margin |>
      Option.iter ~f:begin List.iter ~f:begin fun (c, b) ->
          String.Table.add_exn b_margin c b
        end
      end
    end >>= fun () ->
    Clock_ns.after Time_ns.Span.(of_int_ms 50) >>= fun () ->
    Rest.balances ~buf:buf_json ~all:false ~key ~secret () >>| begin function
    | Error err -> Log.error log_plnx "%s" @@ Rest.Http_error.to_string err
    | Ok bs ->
      String.Table.clear b_exchange;
      List.iter bs ~f:(fun (c, b) -> String.Table.add_exn b_exchange c b)
    end >>| fun () ->
    write_exchange_balance conn;
    write_margin_balance conn

  let update_balances_loop ?start conn span =
    Clock_ns.every
      ?start ~stop:(Writer.close_started conn.w)
      ~continue_on_error:true span
      (fun () -> don't_wait_for @@ update_balances conn)

  let update_connection conn span =
    update_trades_loop conn span;
    update_balances_loop conn span
      ~start:Clock_ns.(after @@ Time_ns.Span.of_int_ms 1000)

  let setup ~addr ~addr_str ~w ~key ~secret:secret_str ~send_secdefs =
    let secret = Cstruct.of_string secret_str in
    let conn = {
      addr ;
      addr_str ;
      w ;
      key ;
      secret ;
      send_secdefs ;
      dropped = 0 ;
      subs = String.Table.create () ;
      subs_depth = String.Table.create () ;
      b_exchange = String.Table.create () ;
      b_margin = String.Table.create () ;
      margin = Rest.create_margin_account_summary () ;
      orders = Int.Table.create () ;
      trades = String.Table.create () ;
      positions = MarginPosition.Set.empty ;
    } in
    Table.set active ~key:addr ~data:conn;
    if key = "" || secret_str = "" then Deferred.return false
    else begin
      Rest.margin_account_summary ~buf:buf_json ~key ~secret () >>| function
      | Error _ -> false
      | Ok _ ->
        update_connection conn !update_client_span;
        true
    end
end

let on_ticker_update buf_cs pair ts t t' =
  let open MarketData in
  let send_update_msgs depth symbol_id w =
    if t.base_volume <> t'.base_volume then begin
      let cs = Cstruct.of_bigarray buf_cs ~len:UpdateSession.sizeof_cs in
      UpdateSession.write ~kind:`Volume ~symbol_id ~data:t'.base_volume cs;
      Writer.write_cstruct w cs
    end;
    if t.low24h <> t'.low24h then begin
      let cs = Cstruct.of_bigarray buf_cs ~len:UpdateSession.sizeof_cs in
      UpdateSession.write ~kind:`Low ~symbol_id ~data:t'.low24h cs;
      Writer.write_cstruct w cs
    end;
    if t.high24h <> t'.high24h then begin
      let cs = Cstruct.of_bigarray buf_cs ~len:UpdateSession.sizeof_cs in
      UpdateSession.write ~kind:`High ~symbol_id ~data:t'.high24h cs;
      Writer.write_cstruct w cs
    end;
    if t.last <> t'.last then begin
      let cs = Cstruct.of_bigarray buf_cs ~len:UpdateLastTradeSnapshot.sizeof_cs in
      UpdateLastTradeSnapshot.write cs ~symbol_id
        ~ts:(float_of_ts ts) ~price:t'.last ~qty:0.;
      Writer.write_cstruct w cs
    end;
    if (t.bid <> t'.bid || t.ask <> t'.ask) && not depth then begin
      let cs = Cstruct.of_bigarray buf_cs ~len:UpdateBidAsk.sizeof_cs in
      UpdateBidAsk.write cs
        ~symbol_id ~ts ~bid:t'.bid ~bid_qty:0. ~ask:t'.ask ~ask_qty:0.;
      Writer.write_cstruct w cs
    end;
  in
  let send_secdef_msg w t =
    let secdef = secdef_of_ticker ~final:true t in
    let secdef_resp_cs = Cstruct.of_bigarray buf_cs ~len:UpdateSession.sizeof_cs in
    SecurityDefinition.Response.to_cstruct secdef_resp_cs secdef;
    Writer.write_cstruct w secdef_resp_cs in
  let on_connection { Connection.addr; addr_str; w; subs; subs_depth; send_secdefs } =
    let on_symbol_id ?(depth=false) symbol_id =
      send_update_msgs depth symbol_id w;
      Log.debug log_dtc "-> %s ticker %s" addr_str pair
    in
    if send_secdefs && phys_equal t t' then send_secdef_msg w t ;
    match String.Table.(find subs pair, find subs_depth pair) with
    | Some sym_id, None -> on_symbol_id ~depth:false sym_id
    | Some sym_id, _ -> on_symbol_id ~depth:true sym_id
    | _ -> ()
  in
  Connection.(Table.iter active ~f:on_connection)

let on_trade_update buf_cs pair ({ DB.ts; side; price; qty } as t) =
  Log.debug log_plnx "<- %s %s" pair (DB.sexp_of_trade t |> Sexplib.Sexp.to_string);
  String.Table.set latest_trades pair t;
  (* Send trade updates to subscribers. *)
  let cs = Cstruct.of_bigarray buf_cs ~len:MarketData.UpdateTrade.sizeof_cs in
  let on_connection { Connection.addr; addr_str; w; subs; _} =
    let on_symbol_id symbol_id =
      MarketData.UpdateTrade.write
        ~symbol_id
        ~side
        ~p:(price // 100_000_000)
        ~v:(qty // 100_000_000)
        ~ts
        cs;
      Writer.write_cstruct w cs;
      Log.debug log_dtc "-> %s %s %s"
        addr_str pair (DB.sexp_of_trade t |> Sexplib.Sexp.to_string);
    in
    Option.iter String.Table.(find subs pair) ~f:on_symbol_id
  in
  Connection.(Table.iter active ~f:on_connection)

let on_book_updates buf_cs pair ts updates =
  let book = String.Table.find_or_add books pair ~default:Book.create in
  let fold_updates { Book.bid; ask } ({ side; price; qty } : DB.book_entry) =
    match side with
    | `Buy -> {
        Book.bid = begin
          if qty > 0 then Int.Map.add bid ~key:price ~data:qty
          else Int.Map.remove bid price
        end;
        ask;
      }
    | `Sell -> {
        Book.ask = begin
          if qty > 0 then Int.Map.add ask ~key:price ~data:qty
          else Int.Map.remove ask price
        end;
        bid;
      }
  in
  let { Book.bid; ask } =  List.fold_left ~init:book updates ~f:fold_updates in
  book.bid <- bid;
  book.ask <- ask;
  let send_depth_updates addr_str w cs symbol_id u =
    Log.debug log_dtc "-> %s depth %s %s"
      addr_str pair (DB.sexp_of_book_entry u |> Sexplib.Sexp.to_string);
    MarketDepth.Update.write
      ~op:(if u.qty = 0 then `Delete else `Insert_update)
      ~p:(u.price // 100_000_000)
      ~v:(u.qty // 100_000_000)
      ~symbol_id
      ~side:u.side
      cs;
    Writer.write_cstruct w cs;
  in
  let cs = Cstruct.of_bigarray buf_cs ~len:MarketDepth.Update.sizeof_cs in
  let on_connection { Connection.addr; addr_str; w; subs; subs_depth; _ } =
    let on_symbol_id symbol_id =
      List.iter updates ~f:(send_depth_updates addr_str w cs symbol_id);
    in
    Option.iter String.Table.(find subs_depth pair) ~f:on_symbol_id
  in
  Connection.(Table.iter active ~f:on_connection)

let subscribe to_ws_w sym =
  Ws.Msgpck.subscribe to_ws_w [sym] >>| function
  | [reqid] -> Int.Table.set reqid_to_sym reqid sym
  | _ -> invalid_argf "subscribe to %s" sym ()

let ws ?heartbeat ?wait_for_pong () =
  let scratch = Bigstring.create 4096 in
  let to_ws, to_ws_w = Pipe.create () in
  let on_ws_msg msg =
    let now = Time_ns.now () in
    match msg with
    | Wamp.Welcome _ ->
      Int.Table.clear reqid_to_sym;
      Int.Table.clear subid_to_sym;
      String.Table.clear sym_to_subid;
      don't_wait_for @@
      Deferred.List.iter ~f:(subscribe to_ws_w) @@ "ticker" :: String.Table.keys tickers
    | Subscribed { reqid; id } ->
      let sym = Int.Table.find_exn reqid_to_sym reqid in
      Int.Table.set subid_to_sym id sym;
      String.Table.set sym_to_subid sym id
    | Event { Wamp.pubid; subid; details; args; kwArgs } ->
      begin match Int.Table.find_exn subid_to_sym subid with
      | "ticker" ->
        let { symbol } as t = Ws.Msgpck.ticker_of_msgpck @@ List args in
        let old_ts, old_t =
          Option.value ~default:(Time_ns.epoch, t) @@ String.Table.find tickers symbol in
        String.Table.set tickers symbol (now, t);
        on_ticker_update scratch symbol now old_t t
      | sym ->
        let iter_f msg = match Ws.Msgpck.of_msgpck msg with
        | Error msg -> failwith msg
        | Ok { typ="newTrade"; data } ->
          let t = Ws.Msgpck.trade_of_msgpck data in
          Log.debug log_plnx "<- %s %s"
            sym @@ Fn.compose Sexp.to_string DB.sexp_of_trade t;
          on_trade_update scratch sym t
        | Ok { typ="orderBookModify"; data } ->
          on_book_updates scratch sym now [Ws.Msgpck.book_of_msgpck data];
        | Ok { typ="orderBookRemove"; data } ->
          on_book_updates scratch sym now [Ws.Msgpck.book_of_msgpck data];
        | Ok { typ } -> failwithf "unexpected message type %s" typ ()
        in
        List.iter args ~f:iter_f
      end
    | msg -> Log.error log_plnx "unknown message"
  in
  let ws = Ws.open_connection ?heartbeat ~log:log_plnx to_ws in
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> Log.error log_plnx "%s" @@ Exn.to_string exn)

let heartbeat addr w ival =
  let open Logon.Heartbeat in
  let cs = Cstruct.create sizeof_cs in
  let rec loop () =
    Clock_ns.after @@ Time_ns.Span.of_int_sec ival >>= fun () ->
    let { Connection.addr_str; dropped; _ } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "-> %s HEARTBEAT" addr_str;
    write ~dropped_msgs:dropped cs;
    Writer.write_cstruct w cs;
    loop ()
  in
  loop ()

let process addr w msg_cs scratchbuf =
  let open Dtc in
  let addr_str = Socket.Address.Inet.to_string addr in
  (* Erase scratchbuf by security. *)
  Bigstring.set_tail_padded_fixed_string
    scratchbuf ~padding:'\x00' ~pos:0 ~len:(Bigstring.length scratchbuf) "";

  match msg_of_enum Cstruct.LE.(get_uint16 msg_cs 2) with
  | Some EncodingRequest ->
    let open Encoding in
    Log.debug log_dtc "<- ENCODING REQUEST";
    let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in
    Response.write response_cs;
    Writer.write_cstruct w response_cs

  | Some LogonRequest ->
    let open Logon in
    let m = Request.read msg_cs in
    let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in
    let send_secdefs = Int32.(bit_and m.integer_1 128l <> 0l) in
    Log.debug log_dtc "<- [%s] %s" addr_str (Request.show m);
    let logon_response ~result_text ~trading_supported =
      Response.create
        ~server_name:"Poloniex"
        ~result:Success
        ~result_text
        ~security_definitions_supported:true
        ~market_data_supported:true
        ~historical_price_data_supported:true
        ~market_depth_supported:true
        ~market_depth_updates_best_bid_and_ask:true
        ~trading_supported
        ~ocr_supported:true
        ~oco_supported:false
        ~bracket_orders_supported:false
        () in
    let accept trading =
      let trading_supported, result_text =
        match trading with
        | Ok msg -> true, Printf.sprintf "Trading enabled: %s" msg
        | Error msg -> false, Printf.sprintf "Trading disabled: %s" msg
      in
      don't_wait_for @@ heartbeat addr w m.Request.heartbeat_interval;
      Response.to_cstruct response_cs (logon_response ~trading_supported ~result_text);
      Writer.write_cstruct w response_cs;
      Log.debug log_dtc "-> [%s] %s" addr_str result_text;
      if send_secdefs then begin
        let open SecurityDefinition in
        let secdef_resp_cs =
          Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in
        String.Table.iter tickers ~f:begin fun (ts, t) ->
          let secdef = secdef_of_ticker ~final:true t in
          SecurityDefinition.Response.to_cstruct secdef_resp_cs secdef;
          Writer.write_cstruct w secdef_resp_cs;
          Log.debug log_dtc "Written secdef %s" t.symbol
        end
      end
    in
    begin match m.username, m.password with
    | "", _ ->
      don't_wait_for begin
        Deferred.ignore @@
        Connection.setup ~addr ~addr_str ~w ~key:"" ~secret:"" ~send_secdefs
      end ;
      accept @@ Result.fail "No credentials"
    | key, secret when secret <> "" ->
      don't_wait_for begin
        Connection.setup
          ~addr ~addr_str ~w ~key ~secret ~send_secdefs >>| function
        | true -> accept @@ Result.return "Valid Poloniex credentials"
        | false -> accept @@ Result.fail "Invalid Poloniex crendentials"
      end
    end

  | Some Heartbeat ->
    let { Connection.addr_str; _ } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- %s HEARTBEAT" addr_str;

  | Some SecurityDefinitionForSymbolRequest ->
    let open Dtc.SecurityDefinition in
    let m = Request.read msg_cs in
    let { Connection.addr_str; _ } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] %s" addr_str (Request.show m);
    let reject_cs = Cstruct.of_bigarray scratchbuf ~len:Reject.sizeof_cs in
    let reject () =
      Log.info log_dtc "-> [%s] Unknown symbol %s" addr_str m.Request.symbol;
      Reject.write reject_cs ~request_id:m.id "Unknown symbol %s" m.Request.symbol;
      Writer.write_cstruct w reject_cs
    in
    if exchange <> m.Request.exchange then reject ()
    else begin match String.Table.find tickers m.Request.symbol with
    | None -> reject ()
    | Some (ts, t) ->
      let open Response in
      let secdef = secdef_of_ticker ~final:true ~request_id:m.id t in
      Log.debug log_dtc "-> [%s] %s" addr_str (show secdef);
      let secdef_resp_cs = Cstruct.of_bigarray scratchbuf ~len:sizeof_cs in
      Response.to_cstruct secdef_resp_cs secdef;
      Writer.write_cstruct w secdef_resp_cs
    end

  | Some MarketDataRequest ->
    let open MarketData in
    let m = Request.read msg_cs in
    Log.debug log_dtc "<- [%s] %s" addr_str (Request.show m);
    let { Connection.addr_str; subs; _ } = Connection.(Table.find_exn active addr) in
    let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let reject k = Printf.ksprintf begin fun reason ->
        Reject.write r_cs ~symbol_id:m.symbol_id "%s" reason;
        Log.debug log_dtc "-> [%s] Market Data Reject: %s" addr_str reason;
        Writer.write_cstruct w r_cs
      end k
    in
    let accept () =
      let snap =
        let open Option.Monad_infix in
        String.Table.find tickers m.Request.symbol >>| fun (ticker_ts, t) ->
        let books = String.Table.find books m.Request.symbol in
        let bb = books >>= fun { bid } -> Int.Map.max_elt bid >>| fun (p, q) ->
          p // 100_000_000, q // 100_000_000 in
        let ba = books >>= fun { ask } -> Int.Map.min_elt ask >>| fun (p, q) ->
          p // 100_000_000, q // 100_000_000 in
        Snapshot.create
          ~symbol_id:m.Request.symbol_id
          ~session_h:t.high24h
          ~session_l:t.low24h
          ~session_v:t.base_volume
          ?bid:(Option.map bb ~f:fst)
          ?bid_qty:(Option.map bb ~f:snd)
          ?ask:(Option.map ba ~f:fst)
          ?ask_qty:(Option.map ba ~f:fst)
          ~last_trade_p:t.last
          ~bid_ask_ts:ticker_ts
          ()
      in
      match snap with
      | None -> reject "No such symbol %s" m.symbol
      | Some snap ->
        String.Table.set subs m.Request.symbol m.Request.symbol_id;
        let snap_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Snapshot.sizeof_cs in
        Snapshot.to_cstruct snap_cs snap;
        Log.debug log_dtc "-> [%s] %s" addr_str (Snapshot.show snap);
        Writer.write_cstruct w snap_cs
    in
    if m.action = Unsubscribe then String.Table.remove subs m.Request.symbol
    else if exchange <> m.exchange then reject "No such exchange %s" m.exchange
    else accept ()

  | Some MarketDepthRequest ->
    let open MarketDepth in
    let m = Request.read msg_cs in
    let { Connection.addr_str; subs_depth; _ } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] %s" addr_str (Request.show m);
    let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let reject k = Printf.ksprintf begin fun reason ->
        Reject.write r_cs ~symbol_id:m.symbol_id "%s" reason;
        Log.debug log_dtc "-> [%s] Market Depth Reject: %s" addr_str reason;
        Writer.write_cstruct w r_cs
      end k
    in
    let accept { Book.bid; ask } =
      String.Table.set subs_depth m.Request.symbol m.Request.symbol_id;
      let snap_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Snapshot.sizeof_cs in
      ignore @@ Int.Map.fold_right bid ~init:1 ~f:begin fun ~key:price ~data:qty lvl ->
        Snapshot.write snap_cs
          ~symbol_id:m.Request.symbol_id
          ~side:`Buy
          ~p:(price // 100_000_000)
          ~v:(qty // 100_000_000)
          ~lvl
          ~first:(lvl = 1)
          ~last:false;
        Writer.write_cstruct w snap_cs;
        succ lvl
      end;
      ignore @@ Int.Map.fold ask ~init:1 ~f:begin fun ~key:price ~data:qty lvl ->
        Snapshot.write snap_cs
          ~symbol_id:m.Request.symbol_id
          ~side:`Sell
          ~p:(price // 100_000_000)
          ~v:(qty // 100_000_000)
          ~lvl
          ~first:(lvl = 1 && Int.Map.is_empty bid) ~last:false;
        Writer.write_cstruct w snap_cs;
        succ lvl
      end;
      Snapshot.write snap_cs
        ~symbol_id:m.Request.symbol_id
        ~p:0.
        ~v:0.
        ~lvl:0
        ~first:false
        ~last:true;
      Writer.write_cstruct w snap_cs
    in
    if m.action = Unsubscribe then String.Table.remove subs_depth m.symbol
    else if exchange <> m.exchange then reject "No such exchange %s" m.exchange
    else if not String.Table.(mem tickers m.symbol) then reject "No such symbol %s" m.symbol
    else begin match String.Table.find books m.symbol with
    | None -> reject "No orderbook for %s-%s" m.symbol m.exchange
    | Some b -> accept b
    end

  | Some HistoricalPriceDataRequest ->
    let open HistoricalPriceData in
    let m = Request.read msg_cs in
    let { Connection.addr_str; _ } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] %s" addr_str (Request.show m);
    let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let reject k = Printf.ksprintf begin fun reason ->
        Reject.write r_cs ~request_id:m.request_id "%s" reason;
        Log.debug log_dtc "-> [%s] HistoricalPriceData Reject\n" addr_str;
        Writer.write_cstruct w r_cs;
      end k
    in
    let accept () =
      let f addr te_r te_w =
        Writer.write_cstruct te_w msg_cs;
        let r_pipe = Reader.pipe te_r in
        let w_pipe = Writer.pipe w in
        Pipe.transfer_id r_pipe w_pipe >>| fun () ->
        Log.debug log_dtc "-> [%s] <historical data>" addr_str
      in
      Tcp.(with_connection (to_file !plnx_historical) f)
    in
    if exchange <> m.Request.exchange then reject "No such exchange"
    else if not String.Table.(mem tickers m.symbol) then reject "No such symbol"
    else don't_wait_for begin
      try_with
      ~name:"historical_with_connection"
      accept >>| function
      | Error _ -> reject "Historical data server unavailable"
      | Ok () -> ()
      end

  | Some OpenOrdersRequest ->
    let open Trading.Order.Open in
    let m = Request.read msg_cs in
    let { Connection.addr_str; orders } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] %s" addr_str (Request.show m);
    let nb_open_orders = Int.Table.length orders in
    let open Trading.Order.Update in
    let update_cs = Cstruct.of_bigarray ~off:0 ~len:sizeof_cs scratchbuf in
    let send_order_update ~key:_ ~data:(symbol, { id; side; price; qty; Rest.starting_qty; } ) i =
      let price = price // 100_000_000 in
      let amount = qty // 100_000_000 in
      let amount_orig = starting_qty // 100_000_000 in
      let status = Dtc.OrderStatus.(if qty = starting_qty then `Open else `Partially_filled) in
      write
        ~nb_msgs:nb_open_orders
        ~msg_number:i
        ~status
        ~reason:Open_orders_request_response
        ~request_id:m.Request.id
        ~symbol
        ~exchange
        (* ~cli_ord_id ?? *)
        ~srv_ord_id:(Int.to_string id)
        ~xch_ord_id:(Int.to_string id)
        ~ord_type:`Limit
        ~side
        ~p1:price
        ~order_qty:amount_orig
        ~filled_qty:(amount_orig -. amount)
        ~remaining_qty:amount
        ~tif:`Good_till_canceled
        update_cs;
      Writer.write_cstruct w update_cs;
      succ i
    in
    let (_:int) = Int.Table.fold orders ~init:1 ~f:send_order_update in
    if nb_open_orders = 0 then begin
      write ~nb_msgs:1 ~msg_number:1 ~request_id:m.Request.id
        ~reason:Open_orders_request_response ~no_orders:true update_cs;
      Writer.write_cstruct w update_cs
    end;
    Log.debug log_dtc "-> [%s] %d order(s)" addr_str nb_open_orders;

  | Some CurrentPositionsRequest ->
    let open Trading.Position in
    let m = Request.read msg_cs in
    let update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in
    let { Connection.addr_str; positions } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] Positions" addr_str;
    let nb_msgs = MarginPosition.Set.length positions in
    let (_:int) = MarginPosition.Set.fold positions ~init:1 ~f:begin fun msg_number { symbol; p={ price; qty } } ->
        Update.write
          ~trade_account:margin_account
          ~nb_msgs
          ~msg_number
          ~request_id:m.Request.id
          ~symbol
          ~exchange
          ~p:(price // 100_000_000)
          ~v:(qty // 100_000_000)
          update_cs;
        Writer.write_cstruct w update_cs;
        succ msg_number
      end
    in
    if nb_msgs = 0 then begin
      Update.write ~nb_msgs:1 ~msg_number:1 ~request_id:m.Request.id ~no_positions:true update_cs;
      Writer.write_cstruct w update_cs
    end;
    Log.debug log_dtc "-> [%s] %d positions" addr_str nb_msgs;

  | Some HistoricalOrderFillsRequest ->
    let open Trading.Order.Fills in
    let { Request.id; srv_order_id; nb_of_days } as m = Request.read msg_cs in
    let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in
    let { Connection.addr_str; key; secret; trades } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] %s" addr_str (Request.show m);
    let send_no_order_fills () =
      Response.write ~request_id:id ~no_order_fills:true ~nb_msgs:1 ~msg_number:1 response_cs;
      Writer.write_cstruct w response_cs
    in
    let send_order_fill ?(nb_msgs=1) ~request_id ~symbol msg_number { Rest.gid; id; ts; price; qty; fee; order_id; side; category } =
      let trade_account = if margin_enabled symbol then margin_account else exchange_account in
      Response.write
        ~trade_account
        ~nb_msgs
        ~msg_number
        ~request_id
        ~symbol
        ~exchange
        ~srv_order_id:Int.(to_string gid)
        ~side
        ~p:(price // 100_000_000)
        ~v:(qty // 100_000_000)
        ~ts
        response_cs;
      Writer.write_cstruct w response_cs;
      succ msg_number
    in
    let nb_ts = String.Table.fold trades ~init:0 ~f:(fun ~key:_ ~data a -> a + TradeHistory.Set.length data) in
    if nb_ts = 0 then send_no_order_fills ()
    else begin
      match srv_order_id with
      | "" -> ignore @@ String.Table.fold trades ~init:1 ~f:begin fun ~key:symbol ~data a ->
          TradeHistory.Set.fold data ~init:a ~f:(send_order_fill ~nb_msgs:nb_ts ~request_id:id ~symbol);
        end
      | srv_ord_id ->
        let srv_ord_id = Int.of_string srv_ord_id in
        begin match String.Table.fold trades ~init:("", None) ~f:begin fun ~key:symbol ~data a ->
            match snd a, (TradeHistory.Set.find data ~f:(fun { gid } -> gid = srv_ord_id)) with
            | _, Some t -> symbol, Some t
            | _ -> a
          end
        with
        | _, None -> send_no_order_fills ()
        | symbol, Some t -> ignore @@ send_order_fill ~request_id:id ~symbol 1 t
        end
    end

  | Some TradeAccountsRequest ->
    let open Account.List in
    let m = Request.read msg_cs in
    let { Connection.addr_str; } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] TradeAccountsRequest" addr_str;
    let response_cs = Cstruct.of_bigarray ~off:0 ~len:Response.sizeof_cs scratchbuf in
    let accounts = [exchange_account; margin_account] in
    let nb_msgs = List.length accounts in
    List.iteri accounts ~f:begin fun i trade_account ->
      let msg_number = succ i in
      Response.write ~request_id:m.Request.id ~msg_number ~nb_msgs ~trade_account response_cs;
      Log.debug log_dtc "-> [%s] TradeAccountResponse: %s (%d/%d)" addr_str trade_account msg_number nb_msgs
    end;

  | Some AccountBalanceRequest ->
    let open Account.Balance in
    let { Dtc.Account.Balance.Request.id; trade_account } = Request.read msg_cs in
    let c = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] AccountBalanceRequest (%s)" addr_str trade_account;
    let reject_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in
    let reject account =
      Reject.write reject_cs ~request_id:id "Unknown account %s" account;
      Writer.write_cstruct w reject_cs;
      Log.debug log_dtc "-> [%s] AccountBalanceReject: unknown account %s" addr_str account
    in
    begin match trade_account with
    | "" ->
      Connection.write_exchange_balance ~request_id:id ~msg_number:1 ~nb_msgs:2 c;
      Connection.write_margin_balance ~request_id:id ~msg_number:2 ~nb_msgs:2 c
    | account when account = exchange_account ->
      Connection.write_exchange_balance ~request_id:id c
    | account when account = margin_account ->
      Connection.write_margin_balance ~request_id:id c
    | account -> reject account
    end

  | Some SubmitNewSingleOrder ->
    let module S = Dtc.Trading.Order.Submit in
    let module U = Dtc.Trading.Order.Update in
    let order_update_cs = Cstruct.of_bigarray ~off:0 ~len:U.sizeof_cs scratchbuf in
    let m = S.read msg_cs in
    let { Connection.addr_str; key; secret } as c = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] %s" addr_str (S.show m);
    let send_order_update ~status ~reason ~id ~filled_qty ~remaining_qty =
      U.write
        ~nb_msgs:1
        ~msg_number:1
        ~status
        ~reason
        ~symbol:m.symbol
        ~exchange
        ~cli_ord_id:m.cli_ord_id
        ~srv_ord_id:(Int.to_string id)
        ~xch_ord_id:(Int.to_string id)
        ~ord_type:`Limit
        ?side:m.side
        ~p1:m.p1
        ~order_qty:m.qty
        ~filled_qty
        ~remaining_qty
        ?tif:m.tif
        order_update_cs;
      Writer.write_cstruct w order_update_cs
    in
    begin try
      let ord_type = Option.value_exn ~message:"submit: empty ord_type" m.ord_type in
      let side = Option.value_exn ~message:"submit: empty side" m.side in
      let tif =
        if ord_type = `Market
        then `Fill_or_kill
        else Option.value_exn ~message:"submit: empty TIF" m.tif
      in
      if exchange <> m.S.exchange then begin
        Dtc_util.Trading.Order.Submit.reject order_update_cs m "Unknown exchange";
        Writer.write_cstruct w order_update_cs;
        raise Exit
      end;
      begin match ord_type with
      | `Limit | `Market -> ()
      | #OrderType.t ->
        Dtc_util.Trading.Order.Submit.reject order_update_cs m "Unsupported order type %s" Sexplib.((Std.sexp_of_option Dtc.OrderType.sexp_of_t m.ord_type) |> Sexp.to_string);
        Writer.write_cstruct w order_update_cs;
        raise Exit
      end;
      begin match tif with
      | `Good_till_canceled | `Fill_or_kill | `Immediate_or_cancel -> ()
      | tif ->
        Dtc_util.Trading.Order.Submit.reject order_update_cs m "Poloniex does not support TIF Good till date time";
        Writer.write_cstruct w order_update_cs;
        raise Exit
      end;
      let price = match ord_type with
      | `Limit -> satoshis_int_of_float_exn m.p1
      | `Market -> begin
        match String.Table.find tickers m.symbol with
        | None ->
          Dtc_util.Trading.Order.Submit.reject order_update_cs m "%s: Unable to get ticker price for %s" m.S.cli_ord_id m.symbol;
          Writer.write_cstruct w order_update_cs;
          raise Exit
        | Some (_, ticker) ->
          satoshis_int_of_float_exn @@ match side with `Buy -> ticker.ask | `Sell -> ticker.bid
        end
      | _ -> assert false
      in
      let qty = satoshis_int_of_float_exn m.qty in
      let margin = margin_enabled m.symbol in
      let order_f =
        if margin then Rest.margin_order ?max_lending_rate:None
        else Rest.order in
      don't_wait_for begin
        order_f ~buf:buf_json ?tif:m.tif ~key ~secret ~side ~symbol:m.symbol ~price ~qty () >>| function
        | Ok { id; trades; amount_unfilled } -> begin match trades, amount_unfilled with
          | [], _ -> send_order_update ~status:`Open ~reason:New_order_accepted ~id ~filled_qty:0. ~remaining_qty:m.qty
          | trades, 0 ->
            send_order_update ~status:`Filled ~reason:Filled ~id ~filled_qty:m.qty ~remaining_qty:0.;
            if margin then don't_wait_for @@ Connection.update_positions c
          | trades, unfilled ->
            let total_qty = satoshis_int_of_float_exn m.qty in
            let filled_qty = List.fold_left trades ~init:0 ~f:(fun a { gid; id; trade } -> a + trade.qty) in
            let remaining_qty = total_qty - filled_qty in
            let filled_qty = filled_qty // 100_000_000 in
            let remaining_qty = remaining_qty // 100_000_000 in
            send_order_update ~status:`Partially_filled ~reason:Partially_filled ~id ~filled_qty ~remaining_qty;
            if margin then don't_wait_for @@ Connection.update_positions c
          end
        | Error Rest.Http_error.Poloniex msg ->
          Dtc_util.Trading.Order.Submit.reject order_update_cs m "%s" msg
        | Error _ ->
          Dtc_util.Trading.Order.Submit.reject order_update_cs m
            "unknown error when trying to submit %s" m.S.cli_ord_id;
          end;
      Writer.write_cstruct w order_update_cs
    with
    | Exit -> ()
    | exn -> Log.error log_dtc "%s" @@ Exn.to_string exn
    end

  | Some CancelOrder ->
    let open Trading.Order in
    let reject m k =
      let order_update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in
      Printf.ksprintf begin fun reason ->
        Update.write
          ~nb_msgs:1
          ~msg_number:1
          ~status:`Open
          ~reason:Cancel_rejected
          ~cli_ord_id:m.Cancel.cli_ord_id
          ~srv_ord_id:m.srv_ord_id
          ~info_text:reason
          order_update_cs;
        Writer.write_cstruct w order_update_cs;
        Log.debug log_dtc "-> [%s] Reject (%s)" addr_str reason
      end k
    in
    let m = Cancel.read msg_cs in
    let { Connection.addr_str; key; secret } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] Order Cancel cli=%s srv=%s" addr_str m.cli_ord_id m.srv_ord_id;
    don't_wait_for @@ begin
      Rest.cancel ~key ~secret Int.(of_string m.srv_ord_id) >>| function
      | Error Rest.Http_error.Poloniex msg -> reject m "%s" msg
      | Error _ ->
        reject m "exception raised while trying to cancel cli=%s srv=%s"
          m.cli_ord_id m.srv_ord_id
      | Ok () -> ()
    end

  | Some CancelReplaceOrder ->
    let open Trading.Order in
    let reject m k =
      let order_update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in
      Printf.ksprintf begin fun reason ->
        Update.write
          ~nb_msgs:1
          ~msg_number:1
          ~reason:Cancel_replace_rejected
          ~cli_ord_id:m.Replace.cli_ord_id
          ~srv_ord_id:m.srv_ord_id
          ?ord_type:m.Replace.ord_type
          ~p1:m.Replace.p1
          ~p2:m.Replace.p2
          ~order_qty:m.Replace.qty
          ?tif:m.Replace.tif
          ~good_till_ts:m.Replace.good_till_ts
          ~info_text:reason
          order_update_cs;
        Writer.write_cstruct w order_update_cs;
        Log.debug log_dtc "-> [%s] CancelReplaceRej: %s" addr_str reason
      end k
    in
    let m = Replace.read msg_cs in
    let { Connection.addr_str; key; secret } = Connection.(Table.find_exn active addr) in
    Log.debug log_dtc "<- [%s] %s" addr_str (Replace.show m);
    if Option.is_some m.Replace.ord_type then
      reject m "Modification of order type is not supported by Poloniex"
    else if Option.is_some m.Replace.tif then
      reject m "Modification of time in force is not supported by Poloniex"
    else
    don't_wait_for @@ begin
      let id = Int.of_string m.srv_ord_id in
      Rest.cancel ~buf:buf_json ~key ~secret id >>| function
      | Error Rest.Http_error.Poloniex msg ->
        reject m "cancel order %s failed: %s" m.srv_ord_id msg
      | Error _ -> reject m "cancel order %s failed" m.srv_ord_id
      | Ok () -> () (* TODO: send order update *)
    end

  | Some _
  | None ->
    let buf = Buffer.create 128 in
    Cstruct.hexdump_to_buffer buf msg_cs;
    Log.error log_dtc "%s" @@ Buffer.contents buf

let dtcserver ~server ~port =
  let server_fun addr r w =
    (* So that process does not allocate all the time. *)
    let scratchbuf = Bigstring.create 1024 in
    let rec handle_chunk consumed buf ~pos ~len =
      if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        Log.debug log_dtc "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen;
        if len < msglen then return @@ `Consumed (consumed, `Need msglen)
        else begin
          let msg_cs = Cstruct.of_bigarray buf ~off:pos ~len:msglen in
          process addr w msg_cs scratchbuf;
          handle_chunk (consumed + msglen) buf (pos + msglen) (len - msglen)
        end
    in
    let on_connection_io_error exn =
      Connection.(Table.remove active addr);
      Log.error log_dtc "on_connection_io_error (%s): %s"
        Socket.Address.(to_string addr) Exn.(to_string exn)
    in
    let cleanup () =
      Log.info log_dtc "client %s disconnected" Socket.Address.(to_string addr);
      Connection.(Table.remove active addr);
      Deferred.all_unit [Writer.close w; Reader.close r]
    in
    Deferred.ignore @@ Monitor.protect ~finally:cleanup begin fun () ->
      Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_connection_io_error;
      Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk 0))
    end
  in
  let on_handler_error_f addr exn =
    Log.error log_dtc "on_handler_error (%s): %s"
      Socket.Address.(to_string addr) Exn.(to_string exn)
  in
  Conduit_async.serve
    ~on_handler_error:(`Call on_handler_error_f)
    server (Tcp.on_port port) server_fun

let main update_client_span' heartbeat wait_for_pong tls port daemon sockfile pidfile logfile loglevel ll_dtc ll_plnx crt_path key_path () =
  update_client_span := Time_ns.Span.of_string update_client_span';
  let heartbeat = Option.map heartbeat ~f:Time_ns.Span.of_string in
  let wait_for_pong = Option.map wait_for_pong ~f:Time_ns.Span.of_string in
  let dtcserver ~server ~port =
    dtcserver ~server ~port >>= fun dtc_server ->
    Log.info log_dtc "DTC server started";
    Tcp.Server.close_finished dtc_server
  in

  plnx_historical := sockfile;
  Log.set_level log_dtc @@ loglevel_of_int @@ max loglevel ll_dtc;
  Log.set_level log_plnx @@ loglevel_of_int @@ max loglevel ll_plnx;

  if daemon then Daemon.daemonize ~cd:"." ();
  stage begin fun `Scheduler_started ->
    Lock_file.create_exn pidfile >>= fun () ->
    Writer.open_file ~append:true logfile >>= fun log_writer ->
    Log.(set_output log_dtc Output.[stderr (); writer `Text log_writer]);
    Log.(set_output log_plnx Output.[stderr (); writer `Text log_writer]);

    let now = Time_ns.now () in
    Rest.currencies () >>| begin function
    | Error err ->
      failwith "Unable to fetch currencies from Poloniex"
    | Ok currs ->
      List.iter currs ~f:(fun (c, t) -> String.Table.set currencies c t)
    end >>= fun () ->
    Rest.tickers () >>| begin function
    | Error _ ->
      failwith "Unable to fetch tickers from Poloniex"
    | Ok ts ->
      List.iter ts ~f:(fun t -> String.Table.set tickers t.symbol (now, t))
    end >>= fun () ->
    conduit_server ~tls ~crt_path ~key_path >>= fun server ->
    Deferred.all_unit [
      loop_log_errors ~log:log_dtc (ws ?heartbeat ?wait_for_pong);
      loop_log_errors ~log:log_dtc (fun () -> dtcserver ~server ~port)
    ]
  end

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "update-client−span" (optional_with_default "10s" string) ~doc:"span Span between client updates (default: 10s)"
    +> flag "-heartbeat" (optional string) ~doc:" Heartbeat period (default: 25s)"
    +> flag "-wait-for-pong" (optional string) ~doc:" max PONG waiting time (default: 5s)"
    +> flag "-tls" no_arg ~doc:" Use TLS"
    +> flag "-port" (optional_with_default 5571 int) ~doc:"int TCP port to use (5571)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-sockfile" (optional_with_default "run/plnx.sock" string) ~doc:"filename UNIX sock to use (run/plnx.sock)"
    +> flag "-pidfile" (optional_with_default "run/plnx.pid" string) ~doc:"filename Path of the pid file (run/plnx.pid)"
    +> flag "-logfile" (optional_with_default "log/plnx.log" string) ~doc:"filename Path of the log file (log/plnx.log)"
    +> flag "-loglevel" (optional_with_default 2 int) ~doc:"1-3 loglevel for DTC"
    +> flag "-loglevel-dtc" (optional_with_default 2 int) ~doc:"1-3 loglevel for DTC"
    +> flag "-loglevel-plnx" (optional_with_default 2 int) ~doc:"1-3 loglevel for PLNX"
    +> flag "-crt-file" (optional_with_default "ssl/bitsouk.com.crt" string) ~doc:"filename crt file to use (TLS)"
    +> flag "-key-file" (optional_with_default "ssl/bitsouk.com.key" string) ~doc:"filename key file to use (TLS)"
  in
  Command.Staged.async ~summary:"Poloniex bridge" spec main

let () = Command.run command
