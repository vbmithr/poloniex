open Core
open Async

open Plnx
module Rest = Plnx_rest
module Ws = Plnx_ws_new

module Encoding = Dtc_pb.Encoding
module DTC = Dtc_pb.Dtcprotocol_piqi

let write_message w (typ : DTC.dtcmessage_type) gen msg =
  let typ =
    Piqirun.(DTC.gen_dtcmessage_type typ |> to_string |> init_from_string |> int_of_varint) in
  let msg = (gen msg |> Piqirun.to_string) in
  let header = Bytes.create 4 in
  Binary_packing.pack_unsigned_16_little_endian ~buf:header ~pos:0 (4 + String.length msg) ;
  Binary_packing.pack_unsigned_16_little_endian ~buf:header ~pos:2 typ ;
  Writer.write w header ;
  Writer.write w msg

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

let my_exchange = "PLNX"
let exchange_account = "exchange"
let margin_account = "margin"
let update_client_span = ref @@ Time_ns.Span.of_int_sec 10
let sc_mode = ref false

let log_plnx =
  Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_dtc =
  Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]

let subid_to_sym : String.t Int.Table.t = Int.Table.create ()

let currencies : Rest.Currency.t String.Table.t = String.Table.create ()
let tickers : (Time_ns.t * Ticker.t) String.Table.t = String.Table.create ()

type book = {
  ts : Time_ns.t ;
  book : Float.t Float.Map.t ;
}

let bids : book String.Table.t = String.Table.create ()
let asks : book String.Table.t = String.Table.create ()
let latest_trades : Trade.t String.Table.t = String.Table.create ()

let buf_json = Bi_outbuf.create 4096

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

let secdef_of_ticker ?request_id ?(final=true) t =
  let request_id = match request_id with
    | Some reqid -> reqid
    | None when !sc_mode -> 110_000_000l
    | None -> 0l in
  let secdef = DTC.default_security_definition_response () in
  secdef.request_id <- Some request_id ;
  secdef.is_final_message <- Some final ;
  secdef.symbol <- Some t.Ticker.symbol ;
  secdef.exchange <- Some my_exchange ;
  secdef.security_type <- Some `security_type_forex ;
  secdef.description <- Some (descr_of_symbol t.symbol) ;
  secdef.min_price_increment <- Some 1e-8 ;
  secdef.currency_value_per_increment <- Some 1e-8 ;
  secdef.price_display_format <- Some `price_display_format_decimal_8 ;
  secdef.has_market_depth_data <- Some true ;
  secdef


module Connection = struct
  type t = {
    addr: string;
    w: Writer.t;
    key: string;
    secret: string;
    mutable dropped: int;
    subs: Int32.t String.Table.t;
    subs_depth: Int32.t String.Table.t;
    (* Balances *)
    b_exchange: Rest.Balance.t String.Table.t;
    b_margin: Float.t String.Table.t;
    mutable margin: Rest.MarginAccountSummary.t;
    (* Orders & Trades *)
    orders: (string * Rest.OpenOrders.t) Int.Table.t;
    trades: Rest.TradeHistory.Set.t String.Table.t;
    positions: Rest.MarginPosition.t String.Table.t;
    send_secdefs : bool ;
  }

  let active : t String.Table.t = String.Table.create ()

  let update_positions { addr; w; key; secret; positions } =
    let write_update ?(price=0.) ?(qty=0.) symbol =
      let update = DTC.default_position_update () in
      update.trade_account <- Some margin_account ;
      update.total_number_messages <- Some 1l ;
      update.message_number <- Some 1l ;
      update.symbol <- Some symbol ;
      update.exchange <- Some my_exchange ;
      update.quantity <- Some qty ;
      update.average_price <- Some price ;
      update.unsolicited <- Some true ;
      write_message w `position_update DTC.gen_position_update update
    in
    Rest.margin_positions ~buf:buf_json ~key ~secret () >>| function
    | Error err ->
      Log.error log_plnx "update positions (%s): %s"
        addr @@ Rest.Http_error.to_string err
    | Ok ps -> List.iter ps ~f:begin fun (symbol, p) ->
        match p with
        | None ->
          String.Table.remove positions symbol ;
          write_update symbol
        | Some ({ price; qty; total; pl; lending_fees; side } as p) ->
          String.Table.set positions ~key:symbol ~data:p ;
          write_update ~price ~qty symbol
      end

  let update_orders { key; secret; orders } =
    Rest.open_orders ~buf:buf_json ~key ~secret () >>|
    Result.map ~f:begin fun os ->
      Int.Table.clear orders;
      List.iter os ~f:begin fun (symbol, os) ->
        List.iter os ~f:begin fun o ->
          Int.Table.set orders o.Rest.OpenOrders.id (symbol, o)
        end
      end
    end

  let update_trades ({ addr; w; key; secret; trades } as conn) =
    Rest.trade_history ~buf:buf_json ~key ~secret () >>= function
    | Error err ->
      return @@ Log.error log_plnx "update trades (%s): %s"
        addr @@ Rest.Http_error.to_string err
    | Ok ts ->
      update_positions conn >>= fun () ->
      Clock_ns.after Time_ns.Span.(of_int_ms 50) >>= fun () ->
      update_orders conn >>| function
      | Error err ->
        Log.error log_plnx "update orders (%s): %s"
          addr @@ Rest.Http_error.to_string err
      | Ok () ->
        List.iter ts ~f:begin fun (symbol, ts) ->
          let old_ts =
            String.Table.find trades symbol |>
            Option.value ~default:Rest.TradeHistory.Set.empty in
          let cur_ts = Rest.TradeHistory.Set.of_list ts in
          let new_ts = Rest.TradeHistory.Set.diff cur_ts old_ts in
          String.Table.set trades symbol cur_ts;
          Rest.TradeHistory.Set.iter new_ts ~f:ignore (* TODO: send order update messages *)
        end

  let update_trades_loop ?start conn span =
    Clock_ns.every
      ?start ~stop:(Writer.close_started conn.w) ~continue_on_error:true span
      (fun () -> don't_wait_for @@ update_trades conn)

  let write_margin_balance
      ?request_id
      ?(nb_msgs=1)
      ?(msg_number=1) { addr; w; b_margin; margin } =
    let securities_value = margin.net_value *. 1e3 in
    let balance = DTC.default_account_balance_update () in
    balance.request_id <- request_id ;
    balance.cash_balance <- Some (margin.total_value *. 1e3) ;
    balance.securities_value <- Some securities_value ;
    balance.margin_requirement <- Some (margin.total_borrowed_value *. 1e3 *. 0.2) ;
    balance.balance_available_for_new_positions <-
      Some (securities_value /. 0.4 -. margin.total_borrowed_value *. 1e3) ;
    balance.account_currency <- Some "mBTC" ;
    balance.total_number_messages <- Int32.of_int nb_msgs ;
    balance.message_number <- Int32.of_int msg_number ;
    balance.trade_account <- Some margin_account ;
    write_message w `account_balance_update DTC.gen_account_balance_update balance ;
    Log.debug log_dtc "-> %s AccountBalanceUpdate %s (%d/%d)"
      addr margin_account msg_number nb_msgs

  let write_exchange_balance
      ?request_id
      ?(nb_msgs=1)
      ?(msg_number=1) { addr; w; b_exchange } =
    let b = String.Table.find b_exchange "BTC" |>
            Option.map ~f:begin fun { Rest.Balance.available; on_orders } ->
              available *. 1e3, (available -. on_orders) *. 1e3
            end
    in
    let securities_value =
      String.Table.fold b_exchange ~init:0.
        ~f:begin fun ~key:_ ~data:{ Rest.Balance.btc_value } a ->
          a +. btc_value end *. 1e3 in
    let balance = DTC.default_account_balance_update () in
    balance.request_id <- request_id ;
    balance.cash_balance <- Option.map b ~f:fst ;
    balance.securities_value <- Some securities_value ;
    balance.margin_requirement <- Some 0. ;
    balance.balance_available_for_new_positions <- Option.map b ~f:snd ;
    balance.account_currency <- Some "mBTC" ;
    balance.total_number_messages <- Int32.of_int nb_msgs ;
    balance.message_number <- Int32.of_int msg_number ;
    balance.trade_account <- Some exchange_account ;
    write_message w `account_balance_update DTC.gen_account_balance_update balance ;
    Log.debug log_dtc "-> %s AccountBalanceUpdate %s (%d/%d)"
      addr exchange_account msg_number nb_msgs

  let update_balances ({ addr; w; key; secret; b_exchange; b_margin } as conn) =
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

  let setup ~addr ~w ~key ~secret ~send_secdefs =
    let conn = {
      addr ;
      w ;
      key ;
      secret ;
      send_secdefs ;
      dropped = 0 ;
      subs = String.Table.create () ;
      subs_depth = String.Table.create () ;
      b_exchange = String.Table.create () ;
      b_margin = String.Table.create () ;
      margin = Rest.MarginAccountSummary.empty ;
      orders = Int.Table.create () ;
      trades = String.Table.create () ;
      positions = String.Table.create () ;
    } in
    String.Table.set active ~key:addr ~data:conn;
    if key = "" || secret = "" then Deferred.return false
    else begin
      Rest.margin_account_summary ~buf:buf_json ~key ~secret () >>| function
      | Error _ -> false
      | Ok _ ->
        update_connection conn !update_client_span;
        true
    end
end

let send_update_msgs depth symbol_id w ts (t:Ticker.t) (t':Ticker.t) =
  if t.base_volume <> t'.base_volume then begin
    let update = DTC.default_market_data_update_session_volume () in
    update.symbol_id <- Some symbol_id ;
    update.volume <- Some (t'.base_volume) ;
    write_message w `market_data_update_session_volume
      DTC.gen_market_data_update_session_volume update
  end;
  if t.low24h <> t'.low24h then begin
    let update = DTC.default_market_data_update_session_low () in
    update.symbol_id <- Some symbol_id ;
    update.price <- Some (t'.low24h) ;
    write_message w `market_data_update_session_low
      DTC.gen_market_data_update_session_low update
  end;
  if t.high24h <> t'.high24h then begin
    let update = DTC.default_market_data_update_session_high () in
    update.symbol_id <- Some symbol_id ;
    update.price <- Some (t'.high24h) ;
    write_message w `market_data_update_session_high
      DTC.gen_market_data_update_session_high update
  end;
  if t.last <> t'.last then begin
    let float_of_ts ts = Time_ns.to_int_ns_since_epoch ts |> Float.of_int |> fun date -> date /. 1e9 in
    let update = DTC.default_market_data_update_last_trade_snapshot () in
    update.symbol_id <- Some symbol_id ;
    update.last_trade_date_time <- Some (float_of_ts ts) ;
    update.last_trade_price <- Some (t'.last) ;
    write_message w `market_data_update_last_trade_snapshot
      DTC.gen_market_data_update_last_trade_snapshot update
  end;
  if (t.bid <> t'.bid || t.ask <> t'.ask) && not depth then begin
    let update = DTC.default_market_data_update_bid_ask () in
    update.symbol_id <- Some symbol_id ;
    update.bid_price <- Some (t'.bid) ;
    update.ask_price <- Some (t'.ask) ;
    write_message w `market_data_update_bid_ask
      DTC.gen_market_data_update_bid_ask update
  end

let on_ticker_update pair ts t t' =
  let send_secdef_msg w t =
    let secdef = secdef_of_ticker ~final:true t in
    write_message w `security_definition_response
      DTC.gen_security_definition_response secdef in
  let on_connection { Connection.addr; w; subs; subs_depth; send_secdefs } =
    let on_symbol_id ?(depth=false) symbol_id =
      send_update_msgs depth symbol_id w ts t t';
      Log.debug log_dtc "-> [%s] %s TICKER" addr pair
    in
    if send_secdefs && phys_equal t t' then send_secdef_msg w t ;
    match String.Table.(find subs pair, find subs_depth pair) with
    | Some sym_id, None -> on_symbol_id ~depth:false sym_id
    | Some sym_id, _ -> on_symbol_id ~depth:true sym_id
    | _ -> ()
  in
  String.Table.iter Connection.active ~f:on_connection

let float_of_time ts = Int64.to_float (Int63.to_int64 (Time_ns.to_int63_ns_since_epoch ts)) /. 1e9
let int64_of_time ts = Int64.(Int63.to_int64 (Time_ns.to_int63_ns_since_epoch ts) / 1_000_000_000L)
let int32_of_time ts = Int32.of_int64_exn (int64_of_time ts)

let at_bid_or_ask_to_dtc : Side.t -> DTC.at_bid_or_ask_enum = function
  | `Buy -> `at_bid
  | `Sell -> `at_ask

let at_bid_or_ask_of_dtc : DTC.at_bid_or_ask_enum -> Side.t = function
  | `at_bid -> `Buy
  | `at_ask -> `Sell
  | _ -> invalid_arg "at_bid_or_ask_of_dtc"

let buy_sell_to_dtc : Side.t -> DTC.buy_sell_enum = function
  | `Buy -> `buy
  | `Sell -> `sell

let buy_sell_of_dtc : DTC.buy_sell_enum -> Side.t = function
  | `buy -> `Buy
  | `sell -> `Sell
  | _ -> invalid_arg "buy_sell_of_dtc"

let on_trade_update pair ({ Trade.ts; side; price; qty } as t) =
  Log.debug log_plnx "<- %s %s" pair (Trade.sexp_of_t t |> Sexplib.Sexp.to_string);
  (* Send trade updates to subscribers. *)
  let on_connection { Connection.addr; w; subs; _} =
    let on_symbol_id symbol_id =
      let update = DTC.default_market_data_update_trade () in
      update.symbol_id <- Some symbol_id ;
      update.at_bid_or_ask <- Some (at_bid_or_ask_to_dtc side) ;
      update.price <- Some price ;
      update.volume <- Some qty ;
      update.date_time <- Some (float_of_time ts) ;
      write_message w `market_data_update_trade
        DTC.gen_market_data_update_trade update ;
      Log.debug log_dtc "-> [%s] %s T %s"
        addr pair (Sexplib.Sexp.to_string (Trade.sexp_of_t t));
    in
    Option.iter String.Table.(find subs pair) ~f:on_symbol_id
  in
  String.Table.iter Connection.active ~f:on_connection

let on_book_updates pair ts updates =
  let { book = bid } = String.Table.find_exn bids pair in
  let { book = ask } = String.Table.find_exn asks pair in
  let fold_updates (bid, ask) { Plnx.Book.side; price; qty } =
    match side with
    | `Buy ->
      (if qty > 0. then Float.Map.add bid ~key:price ~data:qty
       else Float.Map.remove bid price),
      ask
    | `Sell ->
      (if qty > 0. then Float.Map.add ask ~key:price ~data:qty
       else Float.Map.remove ask price),
      bid
  in
  let bid, ask = List.fold_left ~init:(bid, ask) updates ~f:fold_updates in
  String.Table.set bids pair { ts ; book = bid } ;
  String.Table.set asks pair { ts ; book = ask } ;
  let send_depth_updates
      (update : DTC.Market_depth_update_level.t)
      addr_str w symbol_id u =
    Log.debug log_dtc "-> [%s] %s D %s"
      addr_str pair (Sexplib.Sexp.to_string (Plnx.Book.sexp_of_entry u));
    let update_type =
      if u.qty = 0.
      then `market_depth_delete_level
      else `market_depth_insert_update_level in
    update.side <- Some (at_bid_or_ask_to_dtc u.side) ;
    update.update_type <- Some update_type ;
    update.price <- Some u.price ;
    update.quantity <- Some u.qty ;
    write_message w `market_depth_update_level
      DTC.gen_market_depth_update_level update
  in
  let update = DTC.default_market_depth_update_level () in
  let on_connection { Connection.addr; w; subs; subs_depth; _ } =
    let on_symbol_id symbol_id =
      update.symbol_id <- Some symbol_id ;
      List.iter updates ~f:(send_depth_updates update addr w symbol_id);
    in
    Option.iter String.Table.(find subs_depth pair) ~f:on_symbol_id
  in
  String.Table.iter Connection.active ~f:on_connection

let ws ?heartbeat timeout =
  let latest_ts = ref Time_ns.epoch in
  let to_ws, to_ws_w = Pipe.create () in
  let initialized = ref false in
  let on_event subid id now = function
    | Ws.Repr.Snapshot { symbol ; bid ; ask } ->
      Int.Table.set subid_to_sym subid symbol ;
      String.Table.set bids ~key:symbol ~data:{ ts = now ; book = bid } ;
      String.Table.set asks ~key:symbol ~data:{ ts = now ; book = ask }
    | Update entry ->
      let symbol = Int.Table.find_exn subid_to_sym subid in
      on_book_updates symbol now [entry]
    | Trade t ->
      let symbol = Int.Table.find_exn subid_to_sym subid in
      String.Table.set latest_trades symbol t ;
      on_trade_update symbol t
  in
  let on_msg msg =
    let now = Time_ns.now () in
    latest_ts := now ;
    match msg with
    | Ws.Repr.Error msg ->
      Log.error log_plnx "[WS]: %s" msg
    | Event { subid ; id ; events } ->
      if not !initialized then begin
        let symbols = String.Table.keys tickers in
        List.iter symbols ~f:begin fun symbol ->
          Pipe.write_without_pushback to_ws_w (Ws.Repr.Subscribe symbol)
        end ;
        initialized := true
      end ;
      List.iter events ~f:(on_event subid id now)
  in
  let connected = Condition.create () in
  let restart, ws =
    Ws.open_connection ?heartbeat ~log:log_plnx ~connected to_ws in
  let rec handle_init () =
    Condition.wait connected >>= fun () ->
    initialized := false ;
    handle_init () in
  don't_wait_for (handle_init ()) ;
  let watchdog () =
    let now = Time_ns.now () in
    let diff = Time_ns.diff now !latest_ts in
    if Time_ns.(!latest_ts <> epoch) && Time_ns.Span.(diff > timeout) then
      Condition.signal restart () in
  Clock_ns.every timeout watchdog ;
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_msg)
    (fun exn -> Log.error log_plnx "%s" @@ Exn.to_string exn)

let heartbeat addr w ival =
  let ival = Option.value_map ival ~default:60 ~f:Int32.to_int_exn in
  let rec loop () =
    let msg = DTC.default_heartbeat () in
    Clock_ns.after @@ Time_ns.Span.of_int_sec ival >>= fun () ->
    let { Connection.addr; dropped; _ } =
      String.Table.find_exn Connection.active addr in
    Log.debug log_dtc "-> [%s] Heartbeat" addr;
    msg.num_dropped_messages <- Some (Int32.of_int_exn dropped) ;
    write_message w `heartbeat DTC.gen_heartbeat msg ;
    loop ()
  in
  loop ()

let encoding_request addr w req =
  let open Encoding in
  Log.debug log_dtc "<- [%s] Encoding Request" addr ;
  Encoding.(to_string (Response { version = 7 ; encoding = Protobuf })) |>
  Writer.write w ;
  Log.debug log_dtc "-> [%s] Encoding Response" addr

let logon_response ~result_text ~trading_supported =
  let resp = DTC.default_logon_response () in
  resp.server_name <- Some "Poloniex" ;
  resp.protocol_version <- Some 7l ;
  resp.result <- Some `logon_success ;
  resp.result_text <- Some result_text ;
  resp.market_depth_updates_best_bid_and_ask <- Some true ;
  resp.trading_is_supported <- Some trading_supported ;
  resp.ocoorders_supported <- Some false ;
  resp.order_cancel_replace_supported <- Some true ;
  resp.security_definitions_supported <- Some true ;
  resp.historical_price_data_supported <- Some false ;
  resp.market_depth_is_supported <- Some true ;
  resp.bracket_orders_supported <- Some false ;
  resp.market_data_supported <- Some true ;
  resp.symbol_exchange_delimiter <- Some "-" ;
  resp

let logon_request addr w msg =
  let req = DTC.parse_logon_request msg in
  let int1 = Option.value ~default:0l req.integer_1 in
  let int2 = Option.value ~default:0l req.integer_2 in
  let send_secdefs = Int32.(bit_and int1 128l <> 0l) in
  Log.debug log_dtc "<- [%s] Logon Request" addr;
  let accept trading =
    let trading_supported, result_text =
      match trading with
      | Ok msg -> true, Printf.sprintf "Trading enabled: %s" msg
      | Error msg -> false, Printf.sprintf "Trading disabled: %s" msg
    in
    don't_wait_for @@ heartbeat addr w req.heartbeat_interval_in_seconds;
    write_message w `logon_response
      DTC.gen_logon_response (logon_response ~trading_supported ~result_text) ;
    Log.debug log_dtc "-> [%s] Logon Response (%s)" addr result_text ;
    if not !sc_mode || send_secdefs then begin
      String.Table.iter tickers ~f:begin fun (ts, t) ->
        let secdef = secdef_of_ticker ~final:true t in
        write_message w `security_definition_response
          DTC.gen_security_definition_response secdef ;
        Log.debug log_dtc "Written secdef %s" t.symbol
      end
    end
  in
  begin match req.username, req.password, int2 with
    | Some key, Some secret, 0l ->
      don't_wait_for begin
        Connection.setup ~addr ~w ~key ~secret ~send_secdefs >>| function
        | true -> accept @@ Result.return "Valid Poloniex credentials"
        | false -> accept @@ Result.fail "Invalid Poloniex crendentials"
      end
    | _ ->
      don't_wait_for begin
        Deferred.ignore @@
        Connection.setup ~addr ~w ~key:"" ~secret:"" ~send_secdefs
      end ;
      accept @@ Result.fail "No credentials"
  end

let heartbeat addr w msg =
  let { Connection.addr } =
    String.Table.find_exn Connection.active addr in
  Log.debug log_dtc "<- [%s] Heartbeat" addr

let security_definition_request addr w msg =
  let reject addr_str request_id symbol =
    Log.info log_dtc "-> [%s] (req: %ld) Unknown symbol %s" addr_str request_id symbol;
    let rej = DTC.default_security_definition_reject () in
    rej.request_id <- Some request_id ;
    rej.reject_text <- Some (Printf.sprintf "Unknown symbol %s" symbol) ;
    write_message w `security_definition_reject
      DTC.gen_security_definition_reject rej
  in
  let req = DTC.parse_security_definition_for_symbol_request msg in
  match req.request_id, req.symbol, req.exchange with
    | Some request_id, Some symbol, Some exchange ->
      let { Connection.addr } =
        String.Table.find_exn Connection.active addr in
      Log.debug log_dtc "<- [%s] Sec Def Request %ld %s %s"
        addr request_id symbol exchange ;
      if exchange <> my_exchange then reject addr request_id symbol
      else begin match String.Table.find tickers symbol with
        | None -> reject addr request_id symbol
        | Some (ts, t) ->
          let secdef = secdef_of_ticker ~final:true ~request_id t in
          Log.debug log_dtc "-> [%s] Sec Def Response %ld %s %s"
            addr request_id symbol exchange ;
          write_message w `security_definition_response
            DTC.gen_security_definition_response secdef
      end
    | _ -> ()

let reject_market_data_request ?symbol_id addr w k =
  Printf.ksprintf begin fun reject_text ->
    let rej = DTC.default_market_data_reject () in
    rej.symbol_id <- symbol_id ;
    rej.reject_text <- Some reject_text ;
    Log.debug log_dtc "-> [%s] Market Data Reject: %s" addr reject_text;
    write_message w `market_data_reject
      DTC.gen_market_data_reject rej
  end k

let gen_market_data_snap symbol_id symbol exchange addr w =
  Option.map (String.Table.find tickers symbol) ~f:begin fun (ts, t) ->
    let snap = DTC.default_market_data_snapshot () in
    snap.symbol_id <- Some symbol_id ;
    snap.session_high_price <- Some t.high24h ;
    snap.session_low_price <- Some t.low24h ;
    snap.session_volume <- Some t.base_volume ;
    begin match String.Table.find latest_trades symbol with
      | None -> ()
      | Some { gid; id; ts; side; price; qty } ->
        snap.last_trade_price <- Some price ;
        snap.last_trade_volume <- Some qty ;
        snap.last_trade_date_time <- Some (float_of_time ts) ;
    end ;
    begin match String.Table.(find bids symbol, find asks symbol) with
      | Some { ts ; book = bid }, Some { book = ask }  ->
        snap.bid_ask_date_time <- Some (float_of_time ts) ;
        begin match Float.Map.max_elt bid with
          | None -> ()
          | Some (bbp, bbq) ->
            snap.bid_price <- Some bbp ;
            snap.bid_quantity <- Some bbq
        end ;
        begin match Float.Map.min_elt ask with
          | None -> ()
          | Some (bap, baq) ->
            snap.ask_price <- Some bap ;
            snap.ask_quantity <- Some baq
        end
      | _ -> ()
    end ;
    snap
  end

let market_data_request addr w msg =
  let req = DTC.parse_market_data_request msg in
  match req.symbol_id, req.symbol, req.exchange with
  | Some symbol_id, Some symbol, Some exchange ->
    let { Connection.addr; subs; _ } =
      String.Table.find_exn Connection.active addr in
    Log.debug log_dtc "<- [%s] Market Data Req %ld %s %s"
      addr symbol_id symbol exchange ;
    if req.request_action = Some `unsubscribe then
      String.Table.remove subs symbol
    else if exchange <> my_exchange then
      reject_market_data_request addr w ~symbol_id "No such exchange %s" exchange
    else begin
      match gen_market_data_snap symbol_id symbol exchange addr w with
      | None ->
        reject_market_data_request addr w ~symbol_id "No such symbol %s" symbol
      | Some snap ->
        String.Table.set subs symbol symbol_id;
        Log.debug log_dtc "-> [%s] Market Data Snap %ld %s %s"
          addr symbol_id symbol exchange ;
        write_message w `market_data_snapshot
          DTC.gen_market_data_snapshot snap
    end
  | _ ->
    reject_market_data_request addr w
      "Market Data Request: no symbol id, symbol or exchange provided"

let market_depth_accept
    ~conn:{ Connection.addr ; w ; subs_depth }
    ~req
    ~bid:{ ts = bid_ts ; book = bid }
    ~ask:{ ts = bid_ts ; book = ask }
  =
  (* OK because we sanitize before *******************************************)
  let symbol_id = Option.value_exn req.DTC.Market_depth_request.symbol_id in
  let symbol = Option.value_exn req.DTC.Market_depth_request.symbol in
  let exchange = Option.value_exn req.DTC.Market_depth_request.exchange in
  (****************************************************************************)
  let bid_size = Float.Map.length bid in
  let ask_size = Float.Map.length ask in
  let num_levels = Option.value_map req.num_levels ~default:50 ~f:Int32.to_int_exn in
  String.Table.set subs_depth symbol symbol_id;
  let snap = DTC.default_market_depth_snapshot_level () in
  snap.symbol_id <- Some symbol_id ;
  snap.side <- Some `at_bid ;
  snap.is_last_message_in_batch <- Some false ;
  (* ignore @@ Float.Map.fold_right bid ~init:1 ~f:begin fun ~key:price ~data:qty lvl -> *)
  (*   if lvl < num_levels then begin *)
  (*     snap.price <- Some price ; *)
  (*     snap.quantity <- Some qty ; *)
  (*     snap.level <- Some (Int32.of_int_exn lvl) ; *)
  (*     snap.is_first_message_in_batch <- Some (lvl = 1) ; *)
  (*     write_message w `market_depth_snapshot_level *)
  (*       DTC.gen_market_depth_snapshot_level snap *)
  (*   end ; *)
  (*   succ lvl *)
  (* end; *)
  (* snap.side <- Some `at_ask ; *)
  (* ignore @@ Float.Map.fold ask ~init:1 ~f:begin fun ~key:price ~data:qty lvl -> *)
  (*   if lvl < num_levels then begin *)
  (*     snap.price <- Some price ; *)
  (*     snap.quantity <- Some qty ; *)
  (*     snap.level <- Some (Int32.of_int_exn lvl) ; *)
  (*     snap.is_first_message_in_batch <- Some (lvl = 1 && Float.Map.is_empty bid) ; *)
  (*     write_message w `market_depth_snapshot_level *)
  (*       DTC.gen_market_depth_snapshot_level snap *)
  (*   end ; *)
  (*   succ lvl *)
  (* end; *)
  snap.side <- None ;
  snap.price <- None ;
  snap.quantity <- None ;
  snap.level <- None ;
  snap.is_first_message_in_batch <- Some false ;
  snap.is_last_message_in_batch <- Some true ;
  write_message w `market_depth_snapshot_level
    DTC.gen_market_depth_snapshot_level snap ;
  Log.debug log_dtc "-> [%s] Market Depth Snapshot %s %s (%d/%d)"
    addr symbol exchange (Int.min bid_size num_levels) (Int.min ask_size num_levels)

let market_depth_reject addr w symbol_id k = Printf.ksprintf begin fun reject_text ->
    let rej = DTC.default_market_depth_reject () in
    rej.symbol_id <- Some symbol_id ;
    rej.reject_text <- Some reject_text ;
    Log.debug log_dtc "-> [%s] Market Depth Reject: %ld %s"
      addr symbol_id reject_text;
    write_message w `market_depth_reject
      DTC.gen_market_depth_reject rej
  end k

let market_depth_request addr w msg =
  let req = DTC.parse_market_depth_request msg in
  let ({ Connection.addr; subs_depth; _ } as conn) =
    String.Table.find_exn Connection.active addr in
  match req.symbol_id, req.symbol, req.exchange with
  | Some symbol_id, Some symbol, Some exchange ->
    Log.debug log_dtc "<- [%s] Market Depth Request %s %s" addr symbol exchange ;
    if req.request_action = Some `unsubscribe then
      String.Table.remove subs_depth symbol
    else if exchange <> my_exchange then
      market_depth_reject addr w symbol_id "No such exchange %s" exchange
    else if not String.Table.(mem tickers symbol) then
      market_depth_reject addr w symbol_id "No such symbol %s" symbol
    else begin match String.Table.(find bids symbol, find asks symbol) with
      | Some bid, Some ask ->
        market_depth_accept ~conn ~req ~bid ~ask
      | _ ->
        market_depth_reject addr w symbol_id "No orderbook for %s %s" symbol exchange
    end
  | _ -> ()

let open_orders_request addr w msg =
  let req = DTC.parse_open_orders_request msg in
  match req.request_id with
  | Some request_id ->
    let { Connection.addr; orders } =
      String.Table.find_exn Connection.active addr in
    Log.debug log_dtc "<- [%s] Open Orders Request %ld" addr request_id ;
    let nb_open_orders = Int.Table.length orders in
    let send_order_update
        ~key:_ ~data:(symbol, { Rest.OpenOrders.id; side; price; qty; starting_qty; } ) i =
      let resp = DTC.default_order_update () in
      let status = if qty = starting_qty then
          `order_status_open else `order_status_partially_filled in
      resp.request_id <- Some request_id ;
      resp.total_num_messages <- Some (Int32.of_int_exn nb_open_orders) ;
      resp.message_number <- Some i ;
      resp.order_status <- Some status ;
      resp.order_update_reason <- Some `open_orders_request_response ;
      resp.symbol <- Some symbol ;
      resp.exchange <- Some my_exchange ;
      resp.server_order_id <- Some (Int.to_string id) ;
      resp.exchange_order_id <- Some (Int.to_string id) ;
      resp.order_type <- Some `order_type_limit ;
      resp.buy_sell <- Some (buy_sell_to_dtc side) ;
      resp.price1 <- Some price ;
      resp.order_quantity <- Some starting_qty ;
      resp.filled_quantity <- Some (starting_qty -. qty) ;
      resp.remaining_quantity <- Some qty ;
      resp.time_in_force <- Some `tif_good_till_canceled ;
      write_message w `order_update DTC.gen_order_update resp ;
      Int32.succ i
    in
    let (_:Int32.t) = Int.Table.fold orders ~init:1l ~f:send_order_update in
    if nb_open_orders = 0 then begin
      let resp = DTC.default_order_update () in
      resp.total_num_messages <- Some 1l ;
      resp.message_number <- Some 1l ;
      resp.request_id <- Some request_id ;
      resp.order_update_reason <- Some `open_orders_request_response ;
      resp.no_orders <- Some true ;
      write_message w `order_update DTC.gen_order_update resp
    end;
    Log.debug log_dtc "-> [%s] %d order(s)" addr nb_open_orders
  | _ -> ()

let current_positions_request addr w msg =
  let { Connection.addr; positions } =
    String.Table.find_exn Connection.active addr in
  Log.debug log_dtc "<- [%s] Positions" addr;
  let nb_msgs = String.Table.length positions in
  let req = DTC.parse_current_positions_request msg in
  let update = DTC.default_position_update () in
  let (_:Int32.t) =
    String.Table.fold positions
      ~init:1l ~f:begin fun ~key:symbol ~data:{ price; qty } msg_number ->
      update.trade_account <- Some margin_account ;
      update.total_number_messages <- Int32.of_int nb_msgs ;
      update.message_number <- Some msg_number ;
      update.request_id <- req.request_id ;
      update.symbol <- Some symbol ;
      update.exchange <- Some my_exchange ;
      update.average_price <- Some price ;
      update.quantity <- Some qty ;
      write_message w `position_update DTC.gen_position_update update ;
      Int32.succ msg_number
    end
  in
  if nb_msgs = 0 then begin
    update.total_number_messages <- Some 1l ;
    update.message_number <- Some 1l ;
    update.request_id <- req.request_id ;
    update.no_positions <- Some true ;
    write_message w `position_update DTC.gen_position_update update
  end ;
  Log.debug log_dtc "-> [%s] %d positions" addr nb_msgs

let historical_order_fills addr w msg =
  let { Connection.addr; key; secret; trades } =
    String.Table.find_exn Connection.active addr in
  let req = DTC.parse_historical_order_fills_request msg in
  let resp = DTC.default_historical_order_fill_response () in
  Log.debug log_dtc "<- [%s] Historical Order Fills Req" addr ;
  let send_no_order_fills () =
    resp.request_id <- req.request_id ;
    resp.no_order_fills <- Some true ;
    resp.total_number_messages <- Some 1l ;
    resp.message_number <- Some 1l ;
    write_message w `historical_order_fill_response
      DTC.gen_historical_order_fill_response resp
  in
  let send_order_fill ?(nb_msgs=1) ~symbol msg_number
      { Rest.TradeHistory.gid; id; ts; price; qty; fee; order_id; side; category } =
    let trade_account = if margin_enabled symbol then margin_account else exchange_account in
    resp.request_id <- req.request_id ;
    resp.trade_account <- Some trade_account ;
    resp.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
    resp.message_number <- Some msg_number ;
    resp.symbol <- Some symbol ;
    resp.exchange <- Some my_exchange ;
    resp.server_order_id <- Some (Int.to_string gid) ;
    resp.buy_sell <- Some (buy_sell_to_dtc side) ;
    resp.price <- Some price ;
    resp.quantity <- Some qty ;
    resp.date_time <- Some (int64_of_time ts) ;
    write_message w `historical_order_fill_response
      DTC.gen_historical_order_fill_response resp ;
    Int32.succ msg_number
  in
  let nb_trades = String.Table.fold trades ~init:0 ~f:begin fun ~key:_ ~data a ->
      a + Rest.TradeHistory.Set.length data
    end in
  if nb_trades = 0 then send_no_order_fills ()
  else begin
    match req.server_order_id with
    | None -> ignore @@ String.Table.fold trades ~init:1l ~f:begin fun ~key:symbol ~data a ->
        Rest.TradeHistory.Set.fold data ~init:a ~f:(send_order_fill ~nb_msgs:nb_trades ~symbol);
      end
    | Some srv_ord_id ->
      let srv_ord_id = Int.of_string srv_ord_id in
      begin match String.Table.fold trades ~init:("", None) ~f:begin fun ~key:symbol ~data a ->
          match snd a, (Rest.TradeHistory.Set.find data ~f:(fun { gid } -> gid = srv_ord_id)) with
          | _, Some t -> symbol, Some t
          | _ -> a
        end
        with
        | _, None -> send_no_order_fills ()
        | symbol, Some t -> ignore @@ send_order_fill ~symbol 1l t
      end
  end

let trade_account_request addr w msg =
  let req = DTC.parse_trade_accounts_request msg in
  let resp = DTC.default_trade_account_response () in
  let { Connection.addr } =
    String.Table.find_exn Connection.active addr in
  Log.debug log_dtc "<- [%s] TradeAccountsRequest" addr;
  let accounts = [exchange_account; margin_account] in
  let nb_msgs = List.length accounts in
  List.iteri accounts ~f:begin fun i trade_account ->
    let msg_number = Int32.(succ @@ of_int_exn i) in
    resp.request_id <- req.request_id ;
    resp.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
    resp.message_number <- Some msg_number ;
    resp.trade_account <- Some trade_account ;
    write_message w `trade_account_response DTC.gen_trade_account_response resp ;
    Log.debug log_dtc "-> [%s] TradeAccountResponse: %s (%ld/%d)"
      addr trade_account msg_number nb_msgs
  end

let account_balance_request addr w msg =
  let req = DTC.parse_account_balance_request msg in
  let c = String.Table.find_exn Connection.active addr in
  let reject account =
    let rej = DTC.default_account_balance_reject () in
    rej.request_id <- req.request_id ;
    rej.reject_text <- Some ("Unknown account " ^ account) ;
    Log.debug log_dtc "-> [%s] AccountBalanceReject: unknown account %s" c.addr account
  in
  begin match req.trade_account with
    | None ->
      Log.debug log_dtc "<- [%s] AccountBalanceRequest (all accounts)" c.addr ;
      Connection.write_exchange_balance ?request_id:req.request_id ~msg_number:1 ~nb_msgs:2 c;
      Connection.write_margin_balance ?request_id:req.request_id ~msg_number:2 ~nb_msgs:2 c
    | Some account when account = exchange_account ->
      Log.debug log_dtc "<- [%s] AccountBalanceRequest (%s)" c.addr account;
      Connection.write_exchange_balance ?request_id:req.request_id c
    | Some account when account = margin_account ->
      Log.debug log_dtc "<- [%s] AccountBalanceRequest (%s)" c.addr account;
      Connection.write_margin_balance ?request_id:req.request_id c
    | Some account -> reject account
  end

let reject_order
    ~c:{ Connection.w }
    ~(req : DTC.submit_new_single_order)
    k =
  let update = DTC.default_order_update () in
  Printf.ksprintf begin fun info_text ->
    update.client_order_id <- req.client_order_id ;
    update.symbol <- req.symbol ;
    update.exchange <- req.exchange ;
    update.order_status <- Some `order_status_rejected ;
    update.order_update_reason <- Some `new_order_rejected ;
    update.info_text <- Some info_text ;
    update.buy_sell <- req.buy_sell ;
    update.price1 <- req.price1 ;
    update.price2 <- req.price2 ;
    update.time_in_force <- req.time_in_force ;
    update.good_till_date_time <- req.good_till_date_time ;
    update.free_form_text <- req.free_form_text ;
    update.open_or_close <- req.open_or_close ;
    write_message w `order_update DTC.gen_order_update update
  end k

let send_order_update
    ~c:{ Connection.w }
    ~(req : DTC.submit_new_single_order)
    ~exchange_order_id
    ~status
    ~reason
    ~filled_qty
    ~remaining_qty =
  let update = DTC.default_order_update () in
  update.message_number <- Some 1l ;
  update.total_num_messages <- Some 1l ;
  update.order_status <- Some status ;
  update.order_update_reason <- Some reason ;
  update.client_order_id <- req.client_order_id ;
  update.symbol <- req.symbol ;
  update.exchange <- Some my_exchange ;
  update.server_order_id <- Some (Int.to_string exchange_order_id) ;
  update.exchange_order_id <- Some (Int.to_string exchange_order_id) ;
  update.buy_sell <- req.buy_sell ;
  update.price1 <- req.price1 ;
  update.order_quantity <- req.quantity ;
  update.filled_quantity <- Some filled_qty ;
  update.remaining_quantity <- Some remaining_qty ;
  update.time_in_force <- req.time_in_force ;
  write_message w `order_update DTC.gen_order_update update

(* req argument is normalized. *)
let submit_order_api ~c ~(req : DTC.submit_new_single_order) =
  let { Connection.w ; key ; secret } = c in

  (* OK to do this because req is normalized ************************)
  let symbol = Option.value_exn req.symbol in
  let side = Option.value_exn req.buy_sell |> buy_sell_of_dtc in
  let price = Option.value_exn req.price1 in
  let qty = Option.value_exn req.quantity in
  (******************************************************************)
  let margin = margin_enabled symbol in
  let tif = match req.time_in_force with
    | Some `tif_fill_or_kill -> Some `Fill_or_kill
    | Some `tif_immediate_or_cancel -> Some `Immediate_or_cancel
    | _ -> None
  in
  let order_f =
    if margin then Rest.margin_order ?max_lending_rate:None
    else Rest.order
  in
  order_f ~buf:buf_json ?tif ~key ~secret ~side ~symbol ~price ~qty () >>| function
  | Error Rest.Http_error.Poloniex msg ->
    reject_order ~c ~req "%s" msg
  | Error _ ->
    Option.iter req.client_order_id ~f:begin fun id ->
      reject_order ~c ~req "unknown error when trying to submit %s" id
    end
  | Ok { id; trades; amount_unfilled } -> begin
      match trades, amount_unfilled with
      | [], _ ->
        send_order_update ~c ~req
          ~status:`order_status_open
          ~reason:`new_order_accepted
          ~exchange_order_id:id
          ~filled_qty:0.
          ~remaining_qty:qty
      | trades, 0. ->
        send_order_update ~c ~req
          ~status:`order_status_filled
          ~reason:`order_filled
          ~exchange_order_id:id
          ~filled_qty:qty
          ~remaining_qty:0. ;
        if margin then don't_wait_for @@ Connection.update_positions c
      | trades, unfilled ->
        let filled_qty =
          List.fold_left trades ~init:0. ~f:(fun a { qty } -> a +. qty) in
        let remaining_qty = qty -. filled_qty in
        send_order_update ~c ~req
          ~status:`order_status_partially_filled
          ~reason:`order_filled_partially
          ~exchange_order_id:id
          ~filled_qty
          ~remaining_qty ;
        if margin then don't_wait_for @@ Connection.update_positions c
    end

let submit_new_single_order
    ~c
    ~(req : DTC.submit_new_single_order) =
  req.time_in_force <- begin
    match req.order_type with
    | Some `order_type_market -> Some `tif_fill_or_kill
    | _ -> req.time_in_force
  end ;
  begin match req.symbol, req.exchange with
    | Some symbol, Some exchange when
        String.Table.mem tickers symbol && exchange = my_exchange -> ()
    | _ ->
      reject_order ~c ~req "Unknown symbol or exchange" ;
      raise Exit
  end ;
  begin match Option.value ~default:`tif_unset req.time_in_force with
    | `tif_good_till_canceled
    | `tif_fill_or_kill
    | `tif_immediate_or_cancel -> ()
    | `tif_day ->
      req.time_in_force <- Some `tif_good_till_canceled
    | `tif_unset ->
      reject_order ~c ~req "Time in force unset" ;
      raise Exit
    | #DTC.time_in_force_enum ->
      reject_order ~c ~req "Unsupported time in force" ;
      raise Exit
  end ;
  begin match Option.value ~default:`order_type_unset req.order_type, req.price1 with
    | `order_type_market, _ ->
      req.price1 <- Some Float.max_value
    | `order_type_limit, Some price ->
      req.price1 <- Some price
    | `order_type_limit, None ->
      reject_order ~c ~req "Limit order without a price" ;
      raise Exit
    | #DTC.order_type_enum, _ ->
      reject_order ~c ~req "Unsupported order type" ;
      raise Exit
  end ;
  don't_wait_for (submit_order_api ~c ~req)

let submit_new_single_order addr w msg =
  let c = String.Table.find_exn Connection.active addr in
  let req = DTC.parse_submit_new_single_order msg in
  Log.debug log_dtc "<- [%s] Submit New Single Order" c.Connection.addr ;
  try submit_new_single_order ~c ~req with
  | Exit -> ()
  | exn -> Log.error log_dtc "%s" @@ Exn.to_string exn

let reject_cancel_order
    ~c:{ Connection.w }
    ~(req : DTC.cancel_order)
    k =
  let update = DTC.default_order_update () in
  Printf.ksprintf begin fun info_text ->
    update.message_number <- Some 1l ;
    update.total_num_messages <- Some 1l ;
    update.client_order_id <- req.client_order_id ;
    update.server_order_id <- req.server_order_id ;
    update.order_status <- Some `order_status_open ;
    update.order_update_reason <- Some `order_cancel_rejected ;
    update.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update update
  end k

let cancel_order addr w msg =
  let ({ Connection.addr; key; secret } as c) =
    String.Table.find_exn Connection.active addr in
    let req = DTC.parse_cancel_order msg in
    match Option.map req.server_order_id ~f:Int.of_string with
    | None ->
      reject_cancel_order ~c ~req "Server order id not set"
    | Some order_id ->
      Log.debug log_dtc "<- [%s] Order Cancel %d" addr order_id;
      don't_wait_for begin
        Rest.cancel_order ~key ~secret ~order_id () >>| function
        | Error Rest.Http_error.Poloniex msg ->
          reject_cancel_order ~c ~req "%s" msg
        | Error _ ->
          reject_cancel_order ~c ~req
            "exception raised while trying to cancel %d" order_id
        | Ok () -> ()
      end

let reject_cancel_replace_order
    ~c:{ Connection.w ; addr }
    ~(req : DTC.cancel_replace_order)
    k =
  let price1 =
    if Option.value ~default:false req.price1_is_set then req.price1 else None in
  let price2 =
    if Option.value ~default:false req.price2_is_set then req.price2 else None in
  let update = DTC.default_order_update () in
  Printf.ksprintf begin fun info_text ->
    Log.debug log_dtc "-> [%s] Cancel Replace Reject: %s" addr info_text ;
    update.client_order_id <- req.client_order_id ;
    update.server_order_id <- req.server_order_id ;
    update.order_status <- Some `order_status_open ;
    update.order_update_reason <- Some `order_cancel_replace_rejected ;
    update.info_text <- Some info_text ;
    update.message_number <- Some 1l ;
    update.total_num_messages <- Some 1l ;
    update.exchange <- Some my_exchange ;
    update.price1 <- price1 ;
    update.price2 <- price2 ;
    update.order_quantity <- req.quantity ;
    update.time_in_force <- req.time_in_force ;
    update.good_till_date_time <- req.good_till_date_time ;
    write_message w `order_update DTC.gen_order_update update
  end k

let cancel_replace_order addr w msg =
  let c = String.Table.find_exn Connection.active addr in
  let req = DTC.parse_cancel_replace_order msg in
  Log.debug log_dtc "<- [%s] Cancel Replace Order" c.addr ;
  if Option.is_some req.order_type then
    reject_cancel_replace_order ~c ~req
      "Modification of order type is not supported by Poloniex"
  else if Option.is_some req.time_in_force then
    reject_cancel_replace_order ~c ~req
      "Modification of time in force is not supported by Poloniex"
  else
    match Option.map req.server_order_id ~f:Int.of_string, req.price1 with
    | None, _ ->
      reject_cancel_replace_order ~c ~req "Server order id is not set"
    | _, None ->
      reject_cancel_replace_order ~c ~req
        "Order modify without setting a price is not supported by Poloniex"
    | Some order_id, Some price ->
      don't_wait_for begin
        Rest.modify_order ?qty:req.quantity
          ~key:c.key ~secret:c.secret ~price ~order_id () >>| function
        | Error Rest.Http_error.Poloniex msg ->
          reject_cancel_replace_order ~c ~req "cancel order %d failed: %s" order_id msg
        | Error _ ->
          reject_cancel_replace_order ~c ~req "cancel order %d failed" order_id
        | Ok _ ->
          () (* TODO: send order update *)
      end

let dtcserver ~server ~port =
  let server_fun addr r w =
    let addr = Socket.Address.Inet.to_string addr in
    (* So that process does not allocate all the time. *)
    let rec handle_chunk consumed buf ~pos ~len =
      if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        Log.debug log_dtc "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen;
        if len < msglen then return @@ `Consumed (consumed, `Need msglen)
        else begin
          let msgtype_int = Bigstring.unsafe_get_int16_le buf ~pos:(pos+2) in
          let msgtype : DTC.dtcmessage_type =
            DTC.parse_dtcmessage_type (Piqirun.Varint msgtype_int) in
          let msg_str = Bigstring.To_string.subo buf ~pos:(pos+4) ~len:(msglen-4) in
          let msg = Piqirun.init_from_string msg_str in
          begin match msgtype with
            | `encoding_request ->
              begin match (Encoding.read (Bigstring.To_string.subo buf ~pos ~len:16)) with
                | None -> Log.error log_dtc "Invalid encoding request received"
                | Some msg -> encoding_request addr w msg
              end
            | `logon_request -> logon_request addr w msg
            | `heartbeat -> heartbeat addr w msg
            | `security_definition_for_symbol_request -> security_definition_request addr w msg
            | `market_data_request -> market_data_request addr w msg
            | `market_depth_request -> market_depth_request addr w msg
            | `open_orders_request -> open_orders_request addr w msg
            | `current_positions_request -> current_positions_request addr w msg
            | `historical_order_fills_request -> historical_order_fills addr w msg
            | `trade_accounts_request -> trade_account_request addr w msg
            | `account_balance_request -> account_balance_request addr w msg
            | `submit_new_single_order -> submit_new_single_order addr w msg
            | `cancel_order -> cancel_order addr w msg
            | `cancel_replace_order -> cancel_replace_order addr w msg
            | #DTC.dtcmessage_type ->
              Log.error log_dtc "Unknown msg type %d" msgtype_int
          end ;
          handle_chunk (consumed + msglen) buf (pos + msglen) (len - msglen)
        end
    in
    let on_connection_io_error exn =
      String.Table.remove Connection.active addr ;
      Log.error log_dtc "on_connection_io_error (%s): %s" addr Exn.(to_string exn)
    in
    let cleanup () =
      Log.info log_dtc "client %s disconnected" addr ;
      String.Table.remove Connection.active addr ;
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

let loglevel_of_int = function 2 -> `Info | 3 -> `Debug | _ -> `Error

let main update_client_span' heartbeat timeout tls port
    daemon pidfile logfile loglevel ll_dtc ll_plnx crt_path key_path sc () =
  let timeout = Time_ns.Span.of_string timeout in
  sc_mode := sc ;
  update_client_span := Time_ns.Span.of_string update_client_span';
  let heartbeat = Option.map heartbeat ~f:Time_ns.Span.of_string in
  let dtcserver ~server ~port =
    dtcserver ~server ~port >>= fun dtc_server ->
    Log.info log_dtc "DTC server started";
    Tcp.Server.close_finished dtc_server
  in

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
    | Error err -> failwithf "currencies: %s" (Rest.Http_error.to_string err) ()
    | Ok currs ->
      List.iter currs ~f:(fun (c, t) -> String.Table.set currencies c t)
    end >>= fun () ->
    Rest.tickers () >>| begin function
    | Error err -> failwithf "tickers: %s" (Rest.Http_error.to_string err) ()
    | Ok ts ->
      List.iter ts ~f:(fun t -> String.Table.set tickers t.symbol (now, t))
    end >>= fun () ->
    conduit_server ~tls ~crt_path ~key_path >>= fun server ->
    Deferred.all_unit [
      loop_log_errors ~log:log_dtc (fun () -> ws ?heartbeat timeout);
      loop_log_errors ~log:log_dtc (fun () -> dtcserver ~server ~port)
    ]
  end

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-update-client−span" (optional_with_default "10s" string) ~doc:"span Span between client updates (default: 10s)"
    +> flag "-heartbeat" (optional string) ~doc:" WS heartbeat period (default: 25s)"
    +> flag "-timeout" (optional_with_default "60s" string) ~doc:" max Disconnect if no message received in N seconds (default: 60s)"
    +> flag "-tls" no_arg ~doc:" Use TLS"
    +> flag "-port" (optional_with_default 5573 int) ~doc:"int TCP port to use (5573)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-pidfile" (optional_with_default "run/plnx.pid" string) ~doc:"filename Path of the pid file (run/plnx.pid)"
    +> flag "-logfile" (optional_with_default "log/plnx.log" string) ~doc:"filename Path of the log file (log/plnx.log)"
    +> flag "-loglevel" (optional_with_default 2 int) ~doc:"1-3 global loglevel"
    +> flag "-loglevel-dtc" (optional_with_default 2 int) ~doc:"1-3 loglevel for DTC"
    +> flag "-loglevel-plnx" (optional_with_default 2 int) ~doc:"1-3 loglevel for PLNX"
    +> flag "-crt-file" (optional_with_default "ssl/bitsouk.com.crt" string) ~doc:"filename crt file to use (TLS)"
    +> flag "-key-file" (optional_with_default "ssl/bitsouk.com.key" string) ~doc:"filename key file to use (TLS)"
    +> flag "-sc" no_arg ~doc:" Sierra Chart mode."
  in
  Command.Staged.async ~summary:"Poloniex bridge" spec main

let () = Command.run command
