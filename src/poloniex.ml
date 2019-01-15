open Core
open Async

open Plnx
open Bmex_common

module Rest = Plnx_rest
module Ws = Plnx_ws_new

module Encoding = Dtc_pb.Encoding
module DTC = Dtc_pb.Dtcprotocol_piqi

let src =
  Logs.Src.create "poloniex" ~doc:"Poloniex DTC"

let rec loop_log_errors ?log f =
  let rec inner () =
    Monitor.try_with_or_error ~name:"loop_log_errors" f >>= function
    | Ok _ -> assert false
    | Error err ->
      Logs_async.err ~src begin fun m ->
        m "run: %a" Error.pp err
      end >>= fun () ->
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
let update_client_span = ref @@ Time_ns.Span.of_int_sec 30
let sc_mode = ref false

let subid_to_sym : String.t Int.Table.t = Int.Table.create ()

let currencies : Rest.Currency.t String.Table.t = String.Table.create ()
let tickers : (Time_ns.t * Ticker.t) String.Table.t = String.Table.create ()

module Book = struct
  type t = {
    mutable ts : Time_ns.t ;
    mutable book : Float.t Float.Map.t
  }

  let empty = {
    ts = Time_ns.epoch ;
    book = Float.Map.empty ;
  }

  let bids : t String.Table.t = String.Table.create ()
  let asks : t String.Table.t = String.Table.create ()

  let get_bids = String.Table.find_or_add bids ~default:(fun () -> empty)
  let get_asks = String.Table.find_or_add asks ~default:(fun () -> empty)

  let set_bids ~symbol ~ts ~book =
    String.Table.set bids ~key:symbol ~data:{ ts ; book }
  let set_asks ~symbol ~ts ~book =
    String.Table.set asks ~key:symbol ~data:{ ts ; book }
end

let latest_trades : Trade.t String.Table.t = String.Table.create ()
let session_high : Float.t String.Table.t = String.Table.create ()
let session_low : Float.t String.Table.t = String.Table.create ()
let session_volume : Float.t String.Table.t = String.Table.create ()

let _ = Clock_ns.every Time_ns.Span.day begin fun () ->
    String.Table.clear session_high ;
    String.Table.clear session_low ;
    String.Table.clear session_volume
  end

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

let secdef_of_symbol ?request_id ?(final=true) symbol =
  let request_id = match request_id with
    | Some reqid -> reqid
    | None when !sc_mode -> 110_000_000l
    | None -> 0l in
  let secdef = DTC.default_security_definition_response () in
  secdef.request_id <- Some request_id ;
  secdef.is_final_message <- Some final ;
  secdef.symbol <- Some symbol ;
  secdef.exchange <- Some my_exchange ;
  secdef.security_type <- Some `security_type_forex ;
  secdef.description <- Some (descr_of_symbol symbol) ;
  secdef.min_price_increment <- Some 1e-8 ;
  secdef.currency_value_per_increment <- Some 1e-8 ;
  secdef.price_display_format <- Some `price_display_format_decimal_8 ;
  secdef.has_market_depth_data <- Some true ;
  secdef

module RestSync : sig
  type t

  val create : unit -> t

  val push : t -> (unit -> unit Deferred.t) -> unit Deferred.t
  val push_nowait : t -> (unit -> unit Deferred.t) -> unit

  val run : t -> unit

  val start : t -> unit
  val stop : t -> unit

  val is_running : t -> bool

  module Default : sig
    val push : (unit -> unit Deferred.t) -> unit Deferred.t
    val push_nowait : (unit -> unit Deferred.t) -> unit
    val run : unit -> unit
    val start : unit -> unit
    val stop : unit -> unit
    val is_running : unit -> bool
  end
end = struct
  type t = {
    r : (unit -> unit Deferred.t) Pipe.Reader.t ;
    w : (unit -> unit Deferred.t) Pipe.Writer.t ;
    mutable run : bool ;
    condition : unit Condition.t ;
  }

  let create () =
    let r, w = Pipe.create () in
    let condition = Condition.create () in
    { r ; w ; run = true ; condition }

  let push { w } thunk =
    Pipe.write w thunk

  let push_nowait { w } thunk =
    Pipe.write_without_pushback w thunk

  let run { r ; run ; condition } =
    let rec inner () =
      if run then
        Pipe.read r >>= function
        | `Eof -> Deferred.unit
        | `Ok thunk ->
          thunk () >>=
          inner
      else
        Condition.wait condition >>=
        inner
    in
    don't_wait_for (inner ())

  let start t =
    t.run <- true ;
    Condition.signal t.condition ()

  let stop t = t.run <- false
  let is_running { run } = run

  module Default = struct
    let default = create ()

    let push thunk = push default thunk
    let push_nowait thunk = push_nowait default thunk

    let run () = run default
    let start () = start default
    let stop () = stop default
    let is_running () = is_running default
  end
end

module Connection = struct
  type t = {
    addr: string;
    w: Writer.t;
    key: string;
    secret: string;
    mutable dropped: int;
    subs: Int32.t String.Table.t;
    rev_subs : string Int32.Table.t;
    subs_depth: Int32.t String.Table.t;
    rev_subs_depth : string Int32.Table.t;
    (* Balances *)
    b_exchange: Rest.Balance.t String.Table.t;
    b_margin: Float.t String.Table.t;
    mutable margin: Rest.MarginAccountSummary.t;
    (* Orders & Trades *)
    client_orders : DTC.Submit_new_single_order.t Int.Table.t ;
    orders: (string * Rest.OpenOrder.t) Int.Table.t;
    trades: Rest.TradeHistory.Set.t String.Table.t;
    positions: Rest.MarginPosition.t String.Table.t;
    send_secdefs : bool ;
  }

  let active : t String.Table.t = String.Table.create ()

  let find = String.Table.find active
  let find_exn = String.Table.find_exn active
  let set = String.Table.set active
  let remove = String.Table.remove active

  let iter = String.Table.iter active

  let write_position_update ?(price=0.) ?(qty=0.) w symbol =
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

  let update_positions { addr; w; key; secret; positions } =
    Rest.margin_positions ~buf:buf_json ~key ~secret () >>= function
    | Error err ->
      Logs_async.err ~src begin fun m ->
        m "update positions (%s): %a"
          addr Rest.Http_error.pp err
      end
    | Ok ps -> List.iter ps ~f:begin fun (symbol, p) ->
        match p with
        | None ->
          String.Table.remove positions symbol ;
          write_position_update w symbol
        | Some ({ price; qty; total; pl; lending_fees; side } as p) ->
          String.Table.set positions ~key:symbol ~data:p ;
          write_position_update w symbol ~price ~qty:(qty *. 1e4)
      end ;
      Deferred.unit

  let update_orders { addr ; key; secret; orders } =
    Rest.open_orders ~buf:buf_json ~key ~secret () >>= function
    | Error err ->
      Logs_async.err ~src begin fun m ->
        m "update orders (%s): %a" addr Rest.Http_error.pp err
      end
    | Ok os ->
      Int.Table.clear orders;
      List.iter os ~f:begin fun (symbol, os) ->
        List.iter os ~f:begin fun o ->
          Logs.debug ~src begin fun m ->
            m "<- [%s] Add %d in order table" addr o.id
          end ;
          Int.Table.set orders o.id (symbol, o)
        end
      end ;
      Deferred.unit

  let update_trades { addr; key; secret; trades } =
    Rest.trade_history ~buf:buf_json ~key ~secret () >>= function
    | Error err ->
      Logs_async.err ~src begin fun m ->
        m "update trades (%s): %a" addr Rest.Http_error.pp err
      end
    | Ok ts ->
      List.iter ts ~f:begin fun (symbol, ts) ->
        let old_ts =
          String.Table.find trades symbol |>
          Option.value ~default:Rest.TradeHistory.Set.empty in
        let cur_ts = Rest.TradeHistory.Set.of_list ts in
        let new_ts = Rest.TradeHistory.Set.diff cur_ts old_ts in
        String.Table.set trades symbol cur_ts;
        Rest.TradeHistory.Set.iter new_ts ~f:ignore (* TODO: send order update messages *)
      end ;
      Deferred.unit

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
    Logs.debug ~src begin fun m ->
      m "-> %s AccountBalanceUpdate %s (%d/%d)"
        addr margin_account msg_number nb_msgs
    end

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
    Logs.debug ~src begin fun m ->
      m "-> %s AccountBalanceUpdate %s (%d/%d)"
        addr exchange_account msg_number nb_msgs
    end

  let update_margin ({ key ; secret } as conn) =
    Rest.margin_account_summary ~buf:buf_json ~key ~secret () >>= function
    | Error err ->
      Logs_async.err ~src (fun m -> m "%a" Rest.Http_error.pp err)
    | Ok m ->
      conn.margin <- m ;
      Deferred.unit

  let update_positive_balances ({ key ; secret ; b_margin } as conn) =
    Rest.positive_balances ~buf:buf_json ~key ~secret () >>= function
    | Error err ->
      Logs_async.err ~src begin fun m ->
        m "%a" Rest.Http_error.pp err
      end
    | Ok bs ->
      String.Table.clear b_margin;
      List.Assoc.find ~equal:(=) bs Margin |>
      Option.iter ~f:begin
        List.iter ~f:(fun (c, b) -> String.Table.add_exn b_margin c b)
      end ;
      write_margin_balance conn ;
      Deferred.unit

  let update_balances ({ key ; secret ; b_exchange } as conn) =
    Rest.balances ~buf:buf_json ~all:false ~key ~secret () >>= function
    | Error err ->
      Logs_async.err ~src begin fun m ->
        m "%a" Rest.Http_error.pp err
      end
    | Ok bs ->
      String.Table.clear b_exchange;
      List.iter bs ~f:(fun (c, b) -> String.Table.add_exn b_exchange c b) ;
      write_exchange_balance conn ;
      Deferred.unit

  let update_connection conn span =
    Clock_ns.every
      ~stop:(Writer.close_started conn.w)
      ~continue_on_error:true
      span
      begin fun () ->
        let open RestSync.Default in
        push_nowait (fun () -> update_positions conn) ;
        push_nowait (fun () -> update_orders conn) ;
        push_nowait (fun () -> update_trades conn) ;
        push_nowait (fun () -> update_margin conn) ;
        push_nowait (fun () -> update_positive_balances conn) ;
        push_nowait (fun () -> update_balances conn) ;
      end

  let setup ~addr ~w ~key ~secret ~send_secdefs =
    let conn = {
      addr ;
      w ;
      key ;
      secret ;
      send_secdefs ;
      dropped = 0 ;
      subs = String.Table.create () ;
      rev_subs = Int32.Table.create () ;
      subs_depth = String.Table.create () ;
      rev_subs_depth = Int32.Table.create () ;
      b_exchange = String.Table.create () ;
      b_margin = String.Table.create () ;
      margin = Rest.MarginAccountSummary.empty ;
      client_orders = Int.Table.create () ;
      orders = Int.Table.create () ;
      trades = String.Table.create () ;
      positions = String.Table.create () ;
    } in
    set ~key:addr ~data:conn ;
    if key = "" || secret = "" then Deferred.return false
    else begin
      Rest.margin_account_summary ~buf:buf_json ~key ~secret () >>| function
      | Error _ -> false
      | Ok _ ->
        update_connection conn !update_client_span ;
        true
    end
end

let float_of_time ts = Int63.to_float (Time_ns.to_int63_ns_since_epoch ts) /. 1e9
let int63_of_time ts = Int63.(Time_ns.to_int63_ns_since_epoch ts / of_int 1_000_000_000)
let int64_of_time ts = Int63.to_int64 (int63_of_time ts)
let int32_of_time ts = Int63.to_int32_exn (int63_of_time ts)

let at_bid_or_ask_of_depth : Side.t -> DTC.at_bid_or_ask_enum = function
  | `buy -> `at_bid
  | `sell -> `at_ask
  | `buy_sell_unset -> `bid_ask_unset

let at_bid_or_ask_of_trade : Side.t -> DTC.at_bid_or_ask_enum = function
  | `buy -> `at_ask
  | `sell -> `at_bid
  | `buy_sell_unset -> `bid_ask_unset

let depth_update = DTC.default_market_depth_update_level ()
let bidask_update = DTC.default_market_data_update_bid_ask ()
let trade_update = DTC.default_market_data_update_trade ()
let session_low_update = DTC.default_market_data_update_session_low ()
let session_high_update = DTC.default_market_data_update_session_high ()

let on_trade_update pair ({ Trade.ts; side; price; qty } as t) =
  String.Table.set latest_trades pair t ;
  let session_high =
    let h =
      Option.value ~default:Float.min_value (String.Table.find session_high pair) in
    if h < t.price then begin
      String.Table.set session_high pair t.price ;
      Some t.price
    end else None in
  let session_low =
    let l = Option.value ~default:Float.max_value (String.Table.find session_low pair) in
    if l > t.price then begin
      String.Table.set session_low pair t.price ;
      Some t.price
    end else None in
  String.Table.set session_volume pair begin
    match (String.Table.find session_volume pair) with
    | None -> t.qty
    | Some qty -> qty +. t.qty
  end ;
  Logs.debug ~src begin fun m ->
    m "<- %s %a" pair Sexp.pp_hum (Trade.sexp_of_t t)
  end ;
  (* Send trade updates to subscribers. *)
  let on_connection { Connection.addr; w; subs; _} =
    let on_symbol_id symbol_id =
      trade_update.symbol_id <- Some symbol_id ;
      trade_update.at_bid_or_ask <- Some (at_bid_or_ask_of_trade side) ;
      trade_update.price <- Some price ;
      trade_update.volume <- Some qty ;
      trade_update.date_time <- Some (float_of_time ts) ;
      write_message w `market_data_update_trade
        DTC.gen_market_data_update_trade trade_update ;
      Option.iter session_low ~f:begin fun p ->
        session_low_update.symbol_id <- Some symbol_id ;
        session_low_update.price <- Some p ;
        write_message w `market_data_update_session_low
          DTC.gen_market_data_update_session_low session_low_update
      end ;
      Option.iter session_high ~f:begin fun p ->
        session_high_update.symbol_id <- Some symbol_id ;
        session_high_update.price <- Some p ;
        write_message w `market_data_update_session_high
          DTC.gen_market_data_update_session_high session_high_update
      end
    in
    Option.iter String.Table.(find subs pair) ~f:on_symbol_id
  in
  String.Table.iter Connection.active ~f:on_connection

let send_depth_update
    (update : DTC.Market_depth_update_level.t)
    w (u : Plnx.BookEntry.t) =
  let update_type =
    if u.qty = 0.
    then `market_depth_delete_level
    else `market_depth_insert_update_level in
  update.side <- Some (at_bid_or_ask_of_depth u.side) ;
  update.update_type <- Some update_type ;
  update.price <- Some u.price ;
  update.quantity <- Some u.qty ;
  write_message w `market_depth_update_level
    DTC.gen_market_depth_update_level update

let send_bidask_update
    (update : DTC.Market_data_update_bid_ask.t) w bid ask =
  update.bid_price <- Some bid ;
  update.ask_price <- Some ask ;
  write_message w `market_data_update_bid_ask
    DTC.gen_market_data_update_bid_ask update

let on_book_update pair ts ({ Plnx.BookEntry.side; price; qty } as u) =
  let old_bids = (Book.get_bids pair).book in
  let old_asks = (Book.get_asks pair).book in
  let old_best_bid = Option.value_map ~f:fst ~default:Float.min_value (Float.Map.max_elt old_bids) in
  let old_best_ask = Option.value_map ~f:fst ~default:Float.max_value (Float.Map.min_elt old_asks) in
  let book, new_book = match side with
    | `buy_sell_unset -> invalid_arg "on_book_updates: side unset"
    | `buy ->
      let { Book.book } = Book.get_bids pair in
      let new_book =
        (if qty > 0. then Float.Map.set book ~key:price ~data:qty
         else Float.Map.remove book price) in
      Book.set_bids ~symbol:pair ~ts ~book:new_book ;
      book, new_book
    | `sell ->
      let { Book.book } = Book.get_asks pair in
      let new_book =
        (if qty > 0. then Float.Map.set book ~key:price ~data:qty
         else Float.Map.remove book price) in
      Book.set_asks ~symbol:pair ~ts ~book:new_book ;
      book, new_book
  in
  let new_best_bidask = match side with
    | `buy_sell_unset -> None
    | `buy ->
      let new_best_bid =
        Option.value_map ~f:fst ~default:Float.min_value (Float.Map.max_elt new_book) in
      if new_best_bid > old_best_bid then
        Some (new_best_bid, old_best_ask) else None
    | `sell ->
      let new_best_ask =
        Option.value_map ~f:fst ~default:Float.max_value (Float.Map.min_elt new_book) in
      if new_best_ask < old_best_ask then
        Some (old_best_bid, new_best_ask) else None
  in
  let on_connection { Connection.addr; w; subs; subs_depth } =
    let update_depth symbol_id =
      depth_update.symbol_id <- Some symbol_id ;
      Logs.debug ~src begin fun m ->
        m "-> [%s] %s D %a"
          addr pair Sexp.pp_hum (Plnx.BookEntry.sexp_of_t u)
      end ;
      send_depth_update depth_update w u
    in
    let update_bidask symbol_id (bid, ask) =
      bidask_update.symbol_id <- Some symbol_id ;
      Logs.debug ~src begin fun m ->
        m "-> [%s] %s BIDASK %a"
          addr pair Sexp.pp_hum (Plnx.BookEntry.sexp_of_t u)
      end ;
      send_bidask_update bidask_update w bid ask
    in
    match String.Table.(find subs pair, find subs_depth pair) with
    | _, Some symbol_id -> update_depth symbol_id
    | Some symbol_id, _ -> Option.iter new_best_bidask ~f:(update_bidask symbol_id)
    | _ -> ()
  in
  String.Table.iter Connection.active ~f:on_connection

let on_event subid id now = function
  | Ws.Repr.Snapshot { symbol ; bid ; ask } ->
    Int.Table.set subid_to_sym subid symbol ;
    Book.set_bids ~symbol ~ts:now ~book:bid ;
    Book.set_asks ~symbol ~ts:now ~book:ask ;
  | Update entry ->
    let symbol = Int.Table.find_exn subid_to_sym subid in
    on_book_update symbol now entry
  | Trade t ->
    let symbol = Int.Table.find_exn subid_to_sym subid in
    on_trade_update symbol t

let ws ?heartbeat timeout =
  let latest_ts = ref Time_ns.epoch in
  let to_ws, to_ws_w = Pipe.create () in
  let initialized = ref false in
  let on_msg msg =
    let now = Time_ns.now () in
    latest_ts := now ;
    match msg with
    | Ws.Repr.Error msg ->
      Logs.err ~src (fun m -> m "%s" msg) ;
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
    Ws.open_connection ?heartbeat ~connected to_ws in
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
    (fun exn -> Logs.err ~src (fun m -> m "%a" Exn.pp exn))

let heartbeat addr w ival =
  let ival = Option.value_map ival ~default:60 ~f:Int32.to_int_exn in
  let msg = DTC.default_heartbeat () in
  let rec loop () =
    Clock_ns.after @@ Time_ns.Span.of_int_sec ival >>= fun () ->
    match Connection.find addr with
    | None -> Deferred.unit
    | Some { Connection.dropped } ->
      Logs_async.debug ~src begin fun m ->
        m "-> [%s] Heartbeat" addr
      end >>= fun () ->
      msg.num_dropped_messages <- Some (Int32.of_int_exn dropped) ;
      write_message w `heartbeat DTC.gen_heartbeat msg ;
      loop ()
  in
  loop ()

let encoding_request addr w req =
  let open Encoding in
  Logs.debug ~src (fun m -> m "<- [%s] Encoding Request" addr) ;
  Encoding.(to_string (Response { version = 7 ; encoding = Protobuf })) |>
  Writer.write w ;
  Logs.debug ~src (fun m -> m "-> [%s] Encoding Response" addr)

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
  Logs.debug ~src (fun m -> m "<- [%s] Logon Request" addr) ;
  let accept trading =
    let trading_supported, result_text =
      match trading with
      | Ok msg -> true, Printf.sprintf "Trading enabled: %s" msg
      | Error msg -> false, Printf.sprintf "Trading disabled: %s" msg
    in
    don't_wait_for @@ heartbeat addr w req.heartbeat_interval_in_seconds;
    write_message w `logon_response
      DTC.gen_logon_response (logon_response ~trading_supported ~result_text) ;
    Logs.debug ~src (fun m -> m "-> [%s] Logon Response (%s)" addr result_text) ;
    if not !sc_mode || send_secdefs then begin
      String.Table.iter tickers ~f:begin fun (ts, { symbol }) ->
        let secdef = secdef_of_symbol ~final:true symbol in
        write_message w `security_definition_response
          DTC.gen_security_definition_response secdef ;
        Logs.debug ~src (fun m -> m "Written secdef %s" symbol)
      end
    end
  in
  begin match req.username, req.password, int2 with
    | Some key, Some secret, 0l ->
      RestSync.Default.push_nowait begin fun () ->
        Connection.setup ~addr ~w ~key ~secret ~send_secdefs >>| function
        | true -> accept @@ Result.return "Valid Poloniex credentials"
        | false -> accept @@ Result.fail "Invalid Poloniex crendentials"
      end
    | _ ->
      RestSync.Default.push_nowait begin fun () ->
        Connection.setup ~addr ~w ~key:"" ~secret:"" ~send_secdefs >>| fun _ ->
        accept @@ Result.fail "No credentials"
      end
  end

let heartbeat addr w msg =
  (* Log.debug log_dtc "<- [%s] Heartbeat" addr *)
  ()

let security_definition_request addr w msg =
  let reject addr_str request_id symbol =
    Logs.info ~src begin fun m ->
      m "-> [%s] (req: %ld) Unknown symbol %s" addr_str request_id symbol
    end ;
    let rej = DTC.default_security_definition_reject () in
    rej.request_id <- Some request_id ;
    rej.reject_text <- Some (Printf.sprintf "Unknown symbol %s" symbol) ;
    write_message w `security_definition_reject
      DTC.gen_security_definition_reject rej
  in
  let req = DTC.parse_security_definition_for_symbol_request msg in
  match req.request_id, req.symbol, req.exchange with
    | Some request_id, Some symbol, Some exchange ->
      Logs.debug ~src begin fun m ->
        m "<- [%s] Sec Def Request %ld %s %s"
          addr request_id symbol exchange
      end ;
      if exchange <> my_exchange then reject addr request_id symbol
      else begin match String.Table.find tickers symbol with
        | None -> reject addr request_id symbol
        | Some (ts, { symbol }) ->
          let secdef = secdef_of_symbol ~final:true ~request_id symbol in
          Logs.debug ~src begin fun m ->
            m "-> [%s] Sec Def Response %ld %s %s"
              addr request_id symbol exchange
          end ;
          write_message w `security_definition_response
            DTC.gen_security_definition_response secdef
      end
    | _ -> ()

let reject_market_data_request ?id addr w k =
  let rej = DTC.default_market_data_reject () in
  rej.symbol_id <- id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    Logs.debug ~src begin fun m ->
      m "-> [%s] Market Data Reject: %s" addr reject_text
    end ;
    write_message w `market_data_reject DTC.gen_market_data_reject rej
  end k

let write_market_data_snapshot ?id symbol exchange addr w ts t =
  let snap = DTC.default_market_data_snapshot () in
  snap.symbol_id <- id ;
  snap.session_high_price <- String.Table.find session_high symbol ;
  snap.session_low_price <- String.Table.find session_low symbol ;
  snap.session_volume <- String.Table.find session_volume symbol ;
  begin match String.Table.find latest_trades symbol with
    | None -> ()
    | Some { gid; id; ts; side; price; qty } ->
      snap.last_trade_price <- Some price ;
      snap.last_trade_volume <- Some qty ;
      snap.last_trade_date_time <- Some (float_of_time ts) ;
  end ;
  let bid = Book.get_bids symbol in
  let ask = Book.get_asks symbol in
  let ts = Time_ns.max bid.ts ask.ts in
  if ts <> Time_ns.epoch then
    snap.bid_ask_date_time <- Some (float_of_time ts) ;
  Option.iter (Float.Map.max_elt bid.book) ~f:begin fun (price, qty) ->
    snap.bid_price <- Some price ;
    snap.bid_quantity <- Some qty
  end ;
  Option.iter (Float.Map.min_elt ask.book) ~f:begin fun (price, qty) ->
    snap.ask_price <- Some price ;
    snap.ask_quantity <- Some qty
  end ;
  write_message w `market_data_snapshot DTC.gen_market_data_snapshot snap

let market_data_request addr w msg =
  let req = DTC.parse_market_data_request msg in
  let { Connection.subs ; rev_subs } = Connection.find_exn addr in
  match req.request_action,
        req.symbol_id,
        req.symbol,
        req.exchange
  with
  | _, id, _, Some exchange when exchange <> my_exchange ->
    reject_market_data_request ?id addr w "No such exchange %s" exchange
  | _, id, Some symbol, _ when not (String.Table.mem tickers symbol) ->
    reject_market_data_request ?id addr w "No such symbol %s" symbol
  | Some `unsubscribe, Some id, _, _ ->
    Logs.debug ~src begin fun m ->
      m "<- [%s] Market Data Unsubscribe %ld" addr id
    end ;
    Option.iter (Int32.Table.find rev_subs id) ~f:begin fun symbol ->
      Logs.debug ~src begin fun m ->
        m "<- [%s] Market Data Unsubscribe %ld %s" addr id symbol
      end ;
      String.Table.remove subs symbol
    end ;
    Int32.Table.remove rev_subs id
  | Some `snapshot, _, Some symbol, Some exchange ->
    Logs.debug ~src begin fun m ->
      m "-> [%s] Market Data Snapshot %s %s" addr symbol exchange
    end ;
    let ts, t = String.Table.find_exn tickers symbol in
    write_market_data_snapshot symbol exchange addr w ts t
  | Some `subscribe, Some id, Some symbol, Some exchange ->
    Logs.debug ~src begin fun m ->
      m "<- [%s] Market Data Subscribe %ld %s %s"
        addr id symbol exchange
    end ;
    begin
      match Int32.Table.find rev_subs id with
      | Some symbol' when symbol <> symbol' ->
        reject_market_data_request addr w ~id
          "Already subscribed to %s-%s with a different id (was %ld)"
          symbol exchange id
      | _ ->
        String.Table.set subs symbol id ;
        Int32.Table.set rev_subs id symbol ;
        let ts, t = String.Table.find_exn tickers symbol in
        write_market_data_snapshot ~id symbol exchange addr w ts t ;
        Logs.debug ~src begin fun m ->
          m "-> [%s] Market Data Snapshot %s %s" addr symbol exchange
        end
    end
  | _ ->
    reject_market_data_request addr w "invalid request"

let write_market_depth_snapshot ?id addr w ~symbol ~exchange ~num_levels =
  let bid = Book.get_bids symbol in
  let ask = Book.get_asks symbol in
  let bid_size = Float.Map.length bid.book in
  let ask_size = Float.Map.length ask.book in
  let snap = DTC.default_market_depth_snapshot_level () in
  snap.symbol_id <- id ;
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
  Logs.debug ~src begin fun m ->
    m "-> [%s] Market Depth Snapshot %s %s (%d/%d)"
      addr symbol exchange (Int.min bid_size num_levels) (Int.min ask_size num_levels)
  end

let reject_market_depth_request ?id addr w k =
  let rej = DTC.default_market_depth_reject () in
  rej.symbol_id <- id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    Logs.debug ~src begin fun m ->
      m "-> [%s] Market Depth Reject: %s" addr reject_text
    end ;
    write_message w `market_depth_reject
      DTC.gen_market_depth_reject rej
  end k

let market_depth_request addr w msg =
  let req = DTC.parse_market_depth_request msg in
  let num_levels = Option.value_map req.num_levels ~default:50 ~f:Int32.to_int_exn in
  let { Connection.subs_depth ; rev_subs_depth } = Connection.find_exn addr in
  match req.request_action,
        req.symbol_id,
        req.symbol,
        req.exchange
  with
  | _, id, _, Some exchange when exchange <> my_exchange ->
    reject_market_depth_request ?id addr w "No such exchange %s" exchange
  | _, id, Some symbol, _ when not (String.Table.mem tickers symbol) ->
    reject_market_depth_request ?id addr w "No such symbol %s" symbol
  | Some `unsubscribe, Some id, _, _ ->
    Logs.debug ~src begin fun m ->
      m "<- [%s] Market Depth Unsubscribe %ld" addr id
    end ;
    Option.iter (Int32.Table.find rev_subs_depth id) ~f:begin fun symbol ->
      Logs.debug ~src begin fun m ->
        m "<- [%s] Market Depth Unsubscribe %ld %s" addr id symbol
      end ;
      String.Table.remove subs_depth symbol
    end ;
    Int32.Table.remove rev_subs_depth id
  | Some `subscribe, Some id, Some symbol, Some exchange ->
    Logs.debug ~src begin fun m ->
      m "<- [%s] Market Depth Subscribe %ld %s %s" addr id symbol exchange
    end ;
    begin
      match Int32.Table.find rev_subs_depth id with
      | Some symbol' when symbol <> symbol' ->
        reject_market_depth_request addr w ~id
          "Already subscribed to %s-%s with a different id (was %ld)"
          symbol exchange id
      | _ ->
        String.Table.set subs_depth symbol id ;
        Int32.Table.set rev_subs_depth id symbol ;
        write_market_depth_snapshot ~id addr w ~symbol ~exchange ~num_levels
    end
  | _ ->
    reject_market_depth_request addr w "invalid request"

let send_open_order_update w request_id nb_open_orders
    ~key:_ ~data:(symbol, { Rest.OpenOrder.id; side; price; qty; starting_qty; } ) i =
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
  resp.order_type <- Some `order_type_limit ;
  resp.buy_sell <- Some side ;
  resp.price1 <- Some price ;
  resp.order_quantity <- Some (starting_qty *. 1e4) ;
  resp.filled_quantity <- Some ((starting_qty -. qty) *. 1e4) ;
  resp.remaining_quantity <- Some (qty *. 1e4) ;
  resp.time_in_force <- Some `tif_good_till_canceled ;
  write_message w `order_update DTC.gen_order_update resp ;
  Int32.succ i

let open_orders_request addr w msg =
  let req = DTC.parse_open_orders_request msg in
  match req.request_id with
  | Some request_id ->
    let { Connection.orders } = Connection.find_exn addr in
    Logs.debug begin fun m ->
      m "<- [%s] Open Orders Request" addr
    end ;
    let nb_open_orders = Int.Table.length orders in
    let (_:Int32.t) = Int.Table.fold orders
        ~init:1l ~f:(send_open_order_update w request_id nb_open_orders) in
    if nb_open_orders = 0 then begin
      let resp = DTC.default_order_update () in
      resp.total_num_messages <- Some 1l ;
      resp.message_number <- Some 1l ;
      resp.request_id <- Some request_id ;
      resp.order_update_reason <- Some `open_orders_request_response ;
      resp.no_orders <- Some true ;
      write_message w `order_update DTC.gen_order_update resp
    end;
    Logs.debug ~src begin fun m ->
      m "-> [%s] %d order(s)" addr nb_open_orders
    end
  | _ -> ()

let current_positions_request addr w msg =
  let { Connection.positions } = Connection.find_exn addr in
  Logs.debug ~src begin fun m ->
    m "<- [%s] Positions" addr
  end ;
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
  Logs.debug ~src begin fun m ->
    m "-> [%s] %d position(s)" addr nb_msgs
  end

let send_no_order_fills
  w
  (req : DTC.Historical_order_fills_request.t)
  (resp : DTC.Historical_order_fill_response.t) =
  resp.request_id <- req.request_id ;
  resp.no_order_fills <- Some true ;
  resp.total_number_messages <- Some 1l ;
  resp.message_number <- Some 1l ;
  write_message w `historical_order_fill_response
    DTC.gen_historical_order_fill_response resp

let send_order_fill ?(nb_msgs=1) ~symbol
    w
    (req : DTC.Historical_order_fills_request.t)
    (resp : DTC.Historical_order_fill_response.t)
    msg_number
    { Rest.TradeHistory.gid; id; ts; price; qty; fee; order_id; side; category } =
  let trade_account = if margin_enabled symbol then margin_account else exchange_account in
  resp.request_id <- req.request_id ;
  resp.trade_account <- Some trade_account ;
  resp.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
  resp.message_number <- Some msg_number ;
  resp.symbol <- Some symbol ;
  resp.exchange <- Some my_exchange ;
  resp.server_order_id <- Some (Int.to_string gid) ;
  resp.buy_sell <- Some side ;
  resp.price <- Some price ;
  resp.quantity <- Some qty ;
  resp.date_time <- Some (int64_of_time ts) ;
  write_message w `historical_order_fill_response
    DTC.gen_historical_order_fill_response resp ;
  Int32.succ msg_number

let historical_order_fills addr w msg =
  let { Connection.key; secret; trades } = Connection.find_exn addr in
  let req = DTC.parse_historical_order_fills_request msg in
  let resp = DTC.default_historical_order_fill_response () in
  let min_ts =
    Option.value_map req.number_of_days ~default:Time_ns.epoch ~f:begin fun n ->
      Time_ns.(sub (now ()) (Span.of_day (Int32.to_float n)))
    end in
  Logs.debug ~src begin fun m ->
    m "<- [%s] Historical Order Fills Req" addr
  end ;
  let nb_trades = String.Table.fold trades ~init:0 ~f:begin fun ~key:_ ~data a ->
      Rest.TradeHistory.Set.fold data ~init:a ~f:begin fun a t ->
        if Time_ns.(t.Rest.TradeHistory.ts > min_ts) then succ a else a
      end
    end in
  if nb_trades = 0 then send_no_order_fills w req resp
  else begin
    match req.server_order_id with
    | None -> ignore @@ String.Table.fold trades ~init:1l ~f:begin fun ~key:symbol ~data a ->
        Rest.TradeHistory.Set.fold data ~init:a ~f:begin fun a t ->
          if Time_ns.(t.ts > min_ts) then
            send_order_fill ~nb_msgs:nb_trades ~symbol w req resp a t
          else a
        end
      end
    | Some srv_ord_id ->
      let srv_ord_id = Int.of_string srv_ord_id in
      begin
        match String.Table.fold trades ~init:("", None) ~f:begin fun ~key:symbol ~data a ->
            match snd a, (Rest.TradeHistory.Set.find data ~f:(fun { gid } -> gid = srv_ord_id)) with
            | _, Some t -> symbol, Some t
            | _ -> a
        end
        with
        | _, None ->
          send_no_order_fills w req resp
        | symbol, Some t ->
          ignore @@ send_order_fill ~symbol w req resp 1l t
      end
  end

let trade_account_request addr w msg =
  let req = DTC.parse_trade_accounts_request msg in
  let resp = DTC.default_trade_account_response () in
  Logs.debug ~src begin fun m ->
    m "<- [%s] TradeAccountsRequest" addr
  end ;
  let accounts = [exchange_account; margin_account] in
  let nb_msgs = List.length accounts in
  List.iteri accounts ~f:begin fun i trade_account ->
    let msg_number = Int32.(succ @@ of_int_exn i) in
    resp.request_id <- req.request_id ;
    resp.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
    resp.message_number <- Some msg_number ;
    resp.trade_account <- Some trade_account ;
    write_message w `trade_account_response DTC.gen_trade_account_response resp ;
    Logs.debug ~src begin fun m ->
      m "-> [%s] TradeAccountResponse: %s (%ld/%d)"
        addr trade_account msg_number nb_msgs
    end
  end

let reject_account_balance_request addr request_id account =
  let rej = DTC.default_account_balance_reject () in
  rej.request_id <- request_id ;
  rej.reject_text <- Some ("Unknown account " ^ account) ;
  Logs.debug ~src begin fun m ->
    m "-> [%s] AccountBalanceReject: unknown account %s" addr account
  end

let account_balance_request addr w msg =
  let req = DTC.parse_account_balance_request msg in
  let c = Connection.find_exn addr in
  match req.trade_account with
  | None
  | Some "" ->
    Logs.debug ~src begin fun m ->
      m "<- [%s] AccountBalanceRequest (all accounts)" c.addr
    end ;
    Connection.write_exchange_balance ?request_id:req.request_id ~msg_number:1 ~nb_msgs:2 c;
    Connection.write_margin_balance ?request_id:req.request_id ~msg_number:2 ~nb_msgs:2 c
  | Some account when account = exchange_account ->
    Logs.debug ~src begin fun m ->
      m "<- [%s] AccountBalanceRequest (%s)" c.addr account
    end ;
    Connection.write_exchange_balance ?request_id:req.request_id c
  | Some account when account = margin_account ->
    Logs.debug ~src begin fun m ->
      m "<- [%s] AccountBalanceRequest (%s)" c.addr account
    end ;
    Connection.write_margin_balance ?request_id:req.request_id c
  | Some account ->
    reject_account_balance_request addr req.request_id account

let reject_new_order w (req : DTC.submit_new_single_order) k =
  let update = DTC.default_order_update () in
  update.client_order_id <- req.client_order_id ;
  update.symbol <- req.symbol ;
  update.exchange <- req.exchange ;
  update.order_status <- Some `order_status_rejected ;
  update.order_update_reason <- Some `new_order_rejected ;
  update.buy_sell <- req.buy_sell ;
  update.price1 <- req.price1 ;
  update.price2 <- req.price2 ;
  update.time_in_force <- req.time_in_force ;
  update.good_till_date_time <- req.good_till_date_time ;
  update.free_form_text <- req.free_form_text ;
  update.open_or_close <- req.open_or_close ;
  Printf.ksprintf begin fun info_text ->
    update.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update update
  end k

let send_new_order_update w (req : DTC.submit_new_single_order)
    ~server_order_id
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
  update.server_order_id <- Some (Int.to_string server_order_id) ;
  update.buy_sell <- req.buy_sell ;
  update.price1 <- req.price1 ;
  update.order_quantity <- req.quantity ;
  update.filled_quantity <- Some filled_qty ;
  update.remaining_quantity <- Some remaining_qty ;
  update.time_in_force <- req.time_in_force ;
  write_message w `order_update DTC.gen_order_update update

let open_order_of_submit_new_single_order id (req : DTC.Submit_new_single_order.t) margin =
  let side = Option.value ~default:`buy_sell_unset req.buy_sell in
  let price = Option.value ~default:0. req.price1 in
  let qty = Option.value_map req.quantity ~default:0. ~f:(( *. ) 1e-4) in
  let margin = if margin then 1 else 0 in
  Rest.OpenOrder.create ~id ~ts:(Time_ns.now ()) ~side
    ~price ~qty ~starting_qty:qty ~margin

(* req argument is normalized. *)
let submit_new_order conn (req : DTC.submit_new_single_order) =
  let { Connection.addr ; w ; key ; secret ; client_orders ; orders } = conn in
  let symbol = Option.value_exn req.symbol in
  let side = Option.value_exn ~message:"submit_order: side" req.buy_sell in
  let price = Option.value_exn req.price1 in
  let qty = Option.value_map req.quantity ~default:0. ~f:(( *. ) 1e-4) in
  let margin = margin_enabled symbol in
  let tif = match req.time_in_force with
    | Some `tif_fill_or_kill -> Some `Fill_or_kill
    | Some `tif_immediate_or_cancel -> Some `Immediate_or_cancel
    | _ -> None
  in
  let order_f =
    if margin then Rest.submit_margin_order ?max_lending_rate:None
    else Rest.submit_order
  in
  Logs.debug ~src begin fun m ->
    m "-> [%s] Submit Order %s %f %f" addr symbol price qty
  end ;
  order_f ~buf:buf_json ?tif ~key ~secret ~side ~symbol ~price ~qty () >>| function
  | Error Rest.Http_error.Poloniex msg ->
    reject_new_order w req "%s" msg
  | Error err ->
    Option.iter req.client_order_id ~f:begin fun id ->
      reject_new_order w req "%s: %s" id (Rest.Http_error.to_string err)
    end
  | Ok { id; trades; amount_unfilled } -> begin
      Int.Table.set client_orders id req ;
      Int.Table.set orders id
        (symbol, open_order_of_submit_new_single_order id req margin) ;
      Logs.debug ~src begin fun m ->
        m "<- [%s] Submit Order OK %d" addr id
      end ;
      match trades, amount_unfilled with
      | [], _ ->
        send_new_order_update w req
          ~status:`order_status_open
          ~reason:`new_order_accepted
          ~server_order_id:id
          ~filled_qty:0.
          ~remaining_qty:qty
      | trades, 0. ->
        send_new_order_update w req
          ~status:`order_status_filled
          ~reason:`order_filled
          ~server_order_id:id
          ~filled_qty:qty
          ~remaining_qty:0. ;
        if margin then
          RestSync.Default.push_nowait
            (fun () -> Connection.update_positions conn)
      | trades, unfilled ->
        let trades = Rest.OrderResponse.trades_of_symbol trades symbol in
        let filled_qty =
          List.fold_left trades ~init:0. ~f:(fun a { qty } -> a +. qty) in
        let remaining_qty = qty -. filled_qty in
        send_new_order_update w req
          ~status:`order_status_partially_filled
          ~reason:`order_filled_partially
          ~server_order_id:id
          ~filled_qty
          ~remaining_qty ;
        if margin then
          RestSync.Default.push_nowait
            (fun () -> Connection.update_positions conn)
    end

let submit_new_single_order conn (req : DTC.submit_new_single_order) =
  let { Connection.w } = conn in
  req.time_in_force <- begin
    match req.order_type with
    | Some `order_type_market -> Some `tif_fill_or_kill
    | _ -> req.time_in_force
  end ;
  begin match req.symbol, req.exchange with
    | Some symbol, Some exchange when
        String.Table.mem tickers symbol && exchange = my_exchange -> ()
    | _ ->
      reject_new_order w req "Unknown symbol or exchange" ;
      raise Exit
  end ;
  begin match Option.value ~default:`tif_unset req.time_in_force with
    | `tif_good_till_canceled
    | `tif_fill_or_kill
    | `tif_immediate_or_cancel -> ()
    | `tif_day ->
      req.time_in_force <- Some `tif_good_till_canceled
    | `tif_unset ->
      reject_new_order w req "Time in force unset" ;
      raise Exit
    | #DTC.time_in_force_enum ->
      reject_new_order w req "Unsupported time in force" ;
      raise Exit
  end ;
  begin match Option.value ~default:`order_type_unset req.order_type, req.price1 with
    | `order_type_market, _ ->
      req.price1 <-
        Option.(req.symbol >>= String.Table.find latest_trades >>| fun { price } -> price *. 2.)
    | `order_type_limit, Some price ->
      req.price1 <- Some price
    | `order_type_limit, None ->
      reject_new_order w req "Limit order without a price" ;
      raise Exit
    | #DTC.order_type_enum, _ ->
      reject_new_order w req "Unsupported order type" ;
      raise Exit
  end ;
  RestSync.Default.push_nowait (fun () -> submit_new_order conn req)

let submit_new_single_order addr w msg =
  let conn = Connection.find_exn addr in
  let req = DTC.parse_submit_new_single_order msg in
  Logs.debug ~src begin fun m ->
    m "<- [%s] Submit New Single Order" conn.addr
  end ;
  try submit_new_single_order conn req with
  | Exit -> ()
  | exn -> Logs.err ~src (fun m -> m "%a" Exn.pp exn)

let reject_cancel_order w (req : DTC.cancel_order) k =
  let update = DTC.default_order_update () in
  update.message_number <- Some 1l ;
  update.total_num_messages <- Some 1l ;
  update.client_order_id <- req.client_order_id ;
  update.server_order_id <- req.server_order_id ;
  update.order_status <- Some `order_status_open ;
  update.order_update_reason <- Some `order_cancel_rejected ;
  Printf.ksprintf begin fun info_text ->
    update.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update update
  end k

let send_cancel_update w server_order_id (req : DTC.Submit_new_single_order.t) =
  let update = DTC.default_order_update () in
  update.message_number <- Some 1l ;
  update.total_num_messages <- Some 1l ;
  update.symbol <- req.symbol ;
  update.exchange <- req.exchange ;
  update.order_type <- req.order_type ;
  update.buy_sell <- req.buy_sell ;
  update.order_quantity <- req.quantity ;
  update.price1 <- req.price1 ;
  update.price2 <- req.price2 ;
  update.order_status <- Some `order_status_canceled ;
  update.order_update_reason <- Some `order_canceled ;
  update.client_order_id <- req.client_order_id ;
  update.server_order_id <- Some server_order_id ;
  write_message w `order_update DTC.gen_order_update update

let submit_new_single_order_of_open_order symbol (order : Rest.OpenOrder.t) =
  let req = DTC.default_submit_new_single_order () in
  req.symbol <- Some symbol ;
  req.exchange <- Some my_exchange ;
  req.buy_sell <- Some order.side ;
  req.price1 <- Some order.price ;
  req.quantity <- Some order.starting_qty ;
  req

let cancel_order addr w msg =
  let ({ Connection.w ; key ; secret ; client_orders ; orders } as c) =
    Connection.find_exn addr in
    let req = DTC.parse_cancel_order msg in
    match Option.map req.server_order_id ~f:Int.of_string with
    | None ->
      reject_cancel_order w req "Server order id not set"
    | Some order_id ->
      Logs.debug ~src begin fun m ->
        m "<- [%s] Order Cancel %d" addr order_id
      end ;
      RestSync.Default.push_nowait begin fun () ->
        Rest.cancel_order ~key ~secret ~order_id () >>| function
        | Error Rest.Http_error.Poloniex msg ->
          reject_cancel_order w req "%s" msg
        | Error _ ->
          reject_cancel_order w req
            "exception raised while trying to cancel %d" order_id
        | Ok () ->
          Logs.debug ~src begin fun m ->
            m "-> [%s] Order Cancel OK %d" addr order_id
          end ;
          let order_id_str = Int.to_string order_id in
          match Int.Table.find client_orders order_id,
                Int.Table.find orders order_id with
          | None, None ->
            Logs.err ~src begin fun m ->
              m "<- [%s] Unable to find order id %d in tables" addr order_id
            end ;
            send_cancel_update w order_id_str
              (DTC.default_submit_new_single_order ())
          | Some client_order, _ ->
            Int.Table.remove orders order_id ;
            send_cancel_update w order_id_str client_order ;
          | None, Some (symbol, order) ->
            Logs.err ~src begin fun m ->
              m "[%s] Found open order %d but no matching client order" addr order_id
            end ;
            send_cancel_update w order_id_str
              (submit_new_single_order_of_open_order symbol order)
      end

let reject_cancel_replace_order addr w (req : DTC.cancel_replace_order) k =
  let price1 =
    if Option.value ~default:false req.price1_is_set then req.price1 else None in
  let price2 =
    if Option.value ~default:false req.price2_is_set then req.price2 else None in
  let update = DTC.default_order_update () in
  update.client_order_id <- req.client_order_id ;
  update.server_order_id <- req.server_order_id ;
  update.order_status <- Some `order_status_open ;
  update.order_update_reason <- Some `order_cancel_replace_rejected ;
  update.message_number <- Some 1l ;
  update.total_num_messages <- Some 1l ;
  update.exchange <- Some my_exchange ;
  update.price1 <- price1 ;
  update.price2 <- price2 ;
  update.order_quantity <- req.quantity ;
  update.time_in_force <- req.time_in_force ;
  update.good_till_date_time <- req.good_till_date_time ;
  Printf.ksprintf begin fun info_text ->
    Logs.debug ~src begin fun m ->
      m "-> [%s] Cancel Replace Reject: %s" addr info_text
    end ;
    update.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update update
  end k

let send_cancel_replace_update
    ?filled_qty w server_order_id remaining_qty
    (req : DTC.Submit_new_single_order.t)
    (upd : DTC.Cancel_replace_order.t) =
  let update = DTC.default_order_update () in
  let price1_is_set = Option.value ~default:false upd.price1_is_set in
  let price2_is_set = Option.value ~default:false upd.price2_is_set in
  let price1 = match price1_is_set, upd.price1 with
    | true, Some price1 -> Some price1
    | _ -> None in
  let price2 = match price2_is_set, upd.price2 with
    | true, Some price2 -> Some price2
    | _ -> None in
  update.message_number <- Some 1l ;
  update.total_num_messages <- Some 1l ;
  update.symbol <- req.symbol ;
  update.exchange <- req.exchange ;
  update.trade_account <- req.trade_account ;
  update.order_status <- Some `order_status_open ;
  update.order_update_reason <- Some `order_cancel_replace_complete ;
  update.client_order_id <- req.client_order_id ;
  update.previous_server_order_id <- upd.server_order_id ;
  update.server_order_id <- Some server_order_id ;
  update.price1 <- price1 ;
  update.price2 <- price2 ;
  update.order_quantity <- req.quantity ;
  update.filled_quantity <- filled_qty ;
  update.remaining_quantity <- Some remaining_qty ;
  update.order_type <- req.order_type ;
  update.time_in_force <- req.time_in_force ;
  write_message w `order_update DTC.gen_order_update update

let cancel_replace_order addr w msg =
  let { Connection.addr ; w ; key ; secret ; client_orders ; orders }
    = Connection.find_exn addr in
  let req = DTC.parse_cancel_replace_order msg in
  Logs.debug ~src begin fun m ->
    m "<- [%s] Cancel Replace Order" addr
  end ;
  let order_type = Option.value ~default:`order_type_unset req.order_type in
  let tif = Option.value ~default:`tif_unset req.time_in_force in
  if order_type <> `order_type_unset then
    reject_cancel_replace_order addr w req
      "Modification of order type is not supported by Poloniex"
  else if tif <> `tif_unset then
    reject_cancel_replace_order addr w req
      "Modification of time in force is not supported by Poloniex"
  else
    match Option.map req.server_order_id ~f:Int.of_string, req.price1 with
    | None, _ ->
      reject_cancel_replace_order addr w req "Server order id is not set"
    | _, None ->
      reject_cancel_replace_order addr w req
        "Order modify without setting a price is not supported by Poloniex"
    | Some orig_server_id, Some price ->
      let qty = Option.map req.quantity ~f:(( *. ) 1e-4) in
      RestSync.Default.push_nowait begin fun () ->
        Rest.modify_order ~key ~secret ?qty ~price ~order_id:orig_server_id () >>| function
        | Error Rest.Http_error.Poloniex msg ->
          reject_cancel_replace_order addr w req
            "cancel order %d failed: %s" orig_server_id msg
        | Error _ ->
          reject_cancel_replace_order addr w req
            "cancel order %d failed" orig_server_id
        | Ok { id; trades; amount_unfilled } ->
          Logs.debug ~src begin fun m ->
            m "<- [%s] Cancel Replace Order %d -> %d OK" addr orig_server_id id
          end ;
          let order_id_str = Int.to_string id in
          let amount_unfilled = amount_unfilled *. 1e4 in
          match Int.Table.find client_orders orig_server_id,
                Int.Table.find orders orig_server_id with
          | None, None ->
            Logs.err ~src begin fun m ->
              m "[%s] Unable to find order id %d in tables" addr orig_server_id
            end ;
            send_cancel_replace_update w order_id_str amount_unfilled
              (DTC.default_submit_new_single_order ()) req
          | Some client_order, Some (symbol, open_order) ->
            Int.Table.remove client_orders orig_server_id ;
            Int.Table.remove orders orig_server_id ;
            Int.Table.set client_orders id client_order ;
            Int.Table.set orders id (symbol, { open_order with qty = amount_unfilled }) ;
            send_cancel_replace_update
              w order_id_str amount_unfilled client_order req
          | Some client_order, None ->
            Logs.err ~src begin fun m ->
              m "[%s] Found client order %d but no matching open order"
                addr orig_server_id
            end ;
            Int.Table.remove client_orders orig_server_id ;
            Int.Table.set client_orders id client_order ;
            send_cancel_replace_update
              w order_id_str amount_unfilled client_order req
          | None, Some (symbol, order) ->
            Logs.err ~src begin fun m ->
              m "[%s] Found open order %d but no matching client order"
                addr orig_server_id
            end ;
            send_cancel_replace_update w order_id_str amount_unfilled
              (submit_new_single_order_of_open_order symbol order) req
      end

let dtcserver ~server ~port =
  let server_fun addr r w =
    let addr = Socket.Address.Inet.to_string addr in
    (* So that process does not allocate all the time. *)
    let rec handle_chunk consumed buf ~pos ~len =
      if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        (* Log.debug log_dtc "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen; *)
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
                | None -> Logs.err ~src begin fun m ->
                    m "Invalid encoding request received"
                  end
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
              Logs.err ~src begin fun m -> m
                  "Unknown msg type %d" msgtype_int
              end
          end ;
          handle_chunk (consumed + msglen) buf (pos + msglen) (len - msglen)
        end
    in
    let on_connection_io_error exn =
      Connection.remove addr ;
      Logs.err ~src begin fun m ->
        m "on_connection_io_error (%s): %a" addr Exn.pp exn
      end
    in
    let cleanup () =
      Logs_async.info ~src begin fun m ->
        m "client %s disconnected" addr
      end >>= fun () ->
      Connection.remove addr ;
      Deferred.all_unit [Writer.close w; Reader.close r]
    in
    Deferred.ignore @@ Monitor.protect ~finally:cleanup begin fun () ->
      Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_connection_io_error;
      Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk 0))
    end
  in
  let on_handler_error_f addr exn =
    Logs.err ~src begin fun m ->
      m "on_handler_error (%s): %a"
        Socket.Address.(to_string addr) Exn.pp exn
    end
  in
  Conduit_async.serve
    ~on_handler_error:(`Call on_handler_error_f)
    server (Tcp.Where_to_listen.of_port port) server_fun

let loglevel_of_int = function
  | 2 -> Logs.Info
  | 3 -> Debug
  | _ -> Error

let main update_client_span' heartbeat timeout tls port
    pidfile loglevel ll_dtc ll_plnx crt_path key_path sc () =
  List.iter (Logs.Src.list ()) ~f:begin fun s ->
    match Logs.Src.name s with
    | "poloniex" ->
      Logs.Src.set_level s (Some (loglevel_of_int (max loglevel ll_plnx)))
    | _ ->
      Logs.Src.set_level s (Some (loglevel_of_int (max loglevel ll_dtc)))
  end ;
  let timeout = Time_ns.Span.of_string timeout in
  sc_mode := sc ;
  update_client_span := Time_ns.Span.of_string update_client_span';
  let heartbeat = Option.map heartbeat ~f:Time_ns.Span.of_string in
  let dtcserver ~server ~port =
    dtcserver ~server ~port >>= fun dtc_server ->
    Logs_async.info ~src (fun m -> m "DTC server started") >>= fun () ->
    Tcp.Server.close_finished dtc_server
  in
  stage begin fun `Scheduler_started ->
    begin match pidfile with
      | None -> Deferred.unit
      | Some pidfile -> Lock_file.create_exn pidfile
    end >>= fun () ->
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
    RestSync.Default.run () ;
    conduit_server ~tls ~crt_path ~key_path >>= fun server ->
    Deferred.all_unit [
      loop_log_errors (fun () -> ws ?heartbeat timeout) ;
      loop_log_errors (fun () -> dtcserver ~server ~port) ;
    ]
  end

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-update-client−span" (optional_with_default "30s" string) ~doc:"span Span between client updates (default: 10s)"
    +> flag "-heartbeat" (optional string) ~doc:" WS heartbeat period (default: 25s)"
    +> flag "-timeout" (optional_with_default "60s" string) ~doc:" max Disconnect if no message received in N seconds (default: 60s)"
    +> flag "-tls" no_arg ~doc:" Use TLS"
    +> flag "-port" (optional_with_default 5573 int) ~doc:"int TCP port to use (5573)"
    +> flag "-pidfile" (optional string) ~doc:"filename Path of the pid file"
    +> flag "-loglevel" (optional_with_default 2 int) ~doc:"1-3 global loglevel"
    +> flag "-loglevel-dtc" (optional_with_default 2 int) ~doc:"1-3 loglevel for DTC"
    +> flag "-loglevel-plnx" (optional_with_default 2 int) ~doc:"1-3 loglevel for PLNX"
    +> flag "-crt-file" (optional_with_default "ssl/bitsouk.com.crt" string) ~doc:"filename crt file to use (TLS)"
    +> flag "-key-file" (optional_with_default "ssl/bitsouk.com.key" string) ~doc:"filename key file to use (TLS)"
    +> flag "-sc" no_arg ~doc:" Sierra Chart mode."
  in
  Command.Staged.async_spec ~summary:"Poloniex bridge" spec main

let () = Command.run command
