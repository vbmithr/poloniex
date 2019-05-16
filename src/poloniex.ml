open Core
open Async

open Plnx
open Bmex_common
open Poloniex_global
open Poloniex_util

module Rest = Plnx_rest
module Ws = Plnx_ws

module Encoding = Dtc_pb.Encoding
module DTC = Dtc_pb.Dtcprotocol_piqi

let _ = Clock_ns.every Time_ns.Span.day begin fun () ->
    String.Table.clear session_high ;
    String.Table.clear session_low ;
    String.Table.clear session_volume
  end

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

module RestSync = Restsync

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

let on_trade_update pair ({ Trade.ts; side; price; qty; _ } as t) =
  String.Table.set latest_trades ~key:pair ~data:t ;
  let session_high =
    let h =
      Option.value ~default:Float.min_value (String.Table.find session_high pair) in
    if h < t.price then begin
      String.Table.set session_high ~key:pair ~data:t.price ;
      Some t.price
    end else None in
  let session_low =
    let l = Option.value ~default:Float.max_value (String.Table.find session_low pair) in
    if l > t.price then begin
      String.Table.set session_low ~key:pair ~data:t.price ;
      Some t.price
    end else None in
  String.Table.set session_volume ~key:pair ~data:begin
    match (String.Table.find session_volume pair) with
    | None -> t.qty
    | Some qty -> qty +. t.qty
  end ;
  Log.debug begin fun m ->
    m "<- %s %a" pair Sexp.pp_hum (Trade.sexp_of_t t)
  end ;
  (* Send trade updates to subscribers. *)
  let on_connection { Connection.w ; subs ; _ } =
    let on_symbol_id symbol_id =
      trade_update.symbol_id <- Some symbol_id ;
      trade_update.at_bid_or_ask <- Some (at_bid_or_ask_of_trade side) ;
      trade_update.price <- Some price ;
      trade_update.volume <- Some qty ;
      trade_update.date_time <- Some (Ptime.to_float_s ts) ;
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
  Connection.iter ~f:on_connection

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
  let _book, new_book = match side with
    | `buy_sell_unset -> invalid_arg "on_book_updates: side unset"
    | `buy ->
      let { Book.book ; _ } = Book.get_bids pair in
      let new_book =
        (if qty > 0. then Float.Map.set book ~key:price ~data:qty
         else Float.Map.remove book price) in
      Book.set_bids ~symbol:pair ~ts ~book:new_book ;
      book, new_book
    | `sell ->
      let { Book.book ; _ } = Book.get_asks pair in
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
  let on_connection { Connection.addr; w; subs; subs_depth ; _ } =
    let update_depth symbol_id =
      depth_update.symbol_id <- Some symbol_id ;
      Log.debug begin fun m ->
        m "-> [%a] %s D %a"
          pp_print_addr addr pair Sexp.pp_hum (Plnx.BookEntry.sexp_of_t u)
      end ;
      send_depth_update depth_update w u
    in
    let update_bidask symbol_id (bid, ask) =
      bidask_update.symbol_id <- Some symbol_id ;
      Log.debug begin fun m ->
        m "-> [%a] %s BIDASK %a"
          pp_print_addr addr pair Sexp.pp_hum (Plnx.BookEntry.sexp_of_t u)
      end ;
      send_bidask_update bidask_update w bid ask
    in
    match String.Table.(find subs pair, find subs_depth pair) with
    | _, Some symbol_id -> update_depth symbol_id
    | Some symbol_id, _ -> Option.iter new_best_bidask ~f:(update_bidask symbol_id)
    | _ -> ()
  in
  Connection.iter ~f:on_connection

module Actor_ws = struct
  module N = struct
    let base = ["ws"]
    type t = string
    let pp = Format.pp_print_string
    let to_string t = Format.asprintf "%a" pp t
  end
  module E = struct
    type t =
      | Connect
      | Watchdog
      | Close_started
      | Close_finished
      | Snapshot of string
      | Trade of string
      | BookEntry of string

      | TradesPerMinute of int
      | BookEntriesPerMinute of int
    [@@deriving sexp]

    let http_port =
      Option.map ~f:Int.of_string (Sys.getenv "PLNX_WS_HTTP_PORT")

    let warp10_url =
      Option.map ~f:Uri.of_string (Sys.getenv "OVH_METRICS_URL")

    let to_warp10 = function
      | TradesPerMinute i ->
        Option.some @@ Warp10.create
               ~labels:["event", "trades_per_minute"]
               ~name:"poloniex.ws"
               (Warp10.Long (Int64.of_int i))
      | BookEntriesPerMinute i ->
        Option.some @@ Warp10.create
               ~labels:["event", "bookentries_per_minute"]
               ~name:"poloniex.ws"
               (Warp10.Long (Int64.of_int i))
      | _ -> None

    let dummy = Connect
    let level = function
      | BookEntry _ -> Logs.Info
      | _ -> Logs.Info

    let pp ppf v = Sexplib.Sexp.pp ppf (sexp_of_t v)
  end
  module R = struct
    type 'a t = {
      ret: 'a ;
      ts: Time_ns.t ;
      evt: Plnx_ws.t ;
    }
    type view = { ts: Time_ns.t ; evt: Plnx_ws.t } [@@deriving sexp]
    let view { ts ; evt ; ret = _ } = { ts ; evt }
    let pp ppf v = Sexplib.Sexp.pp ppf (sexp_of_view v)
  end
  module V = struct
    type state_ =
      { r : Plnx_ws.t Pipe.Reader.t ;
        w : Plnx_ws.command Pipe.Writer.t ;
        cleaned_up : unit Deferred.t ;
      }

    type state = state_ ref
    type parameters = unit
    type view = unit
    let view _ () = ()
    let pp ppf _ = Format.pp_print_string ppf ""
  end
  module A = Actor.Make(N)(E)(R)(V)
  include A

  module Handlers : HANDLERS
    with type self = bounded queue t = struct
    type self = bounded queue t

    let on_request self { R.ts ; evt = { chanid; seqnum = _; events } ; ret } =
      List.iter events ~f:begin function
        | Ws.Snapshot { symbol ; bid ; ask } ->
          record_event self (Snapshot symbol) ;
          Int.Table.set subid_to_sym ~key:chanid ~data:symbol ;
          Book.set_bids ~symbol ~ts ~book:bid ;
          Book.set_asks ~symbol ~ts ~book:ask ;
        | BookEntry entry ->
          let symbol = Int.Table.find_exn subid_to_sym chanid in
          record_event self (BookEntry symbol) ;
          on_book_update symbol ts entry
        | Trade t ->
          let symbol = Int.Table.find_exn subid_to_sym chanid in
          record_event self (Trade symbol) ;
          on_trade_update symbol t
        | Ticker _ -> ()
      end ;
      return ret

    let init_connection self =
      let symbols = String.Table.keys tickers in
      Plnx_ws_async.connect () >>= fun (r, w, cleaned_up) ->
      log_event self Connect >>= fun () ->
      List.iter symbols ~f:begin fun symbol ->
        Pipe.write_without_pushback w
          (Ws.Subscribe (`String symbol, None))
      end ;
      let push_request evt =
        let ts = Time_ns.now () in
        push_request self { ts ; evt ; ret = () } in
      don't_wait_for (Pipe.iter r ~f:push_request) ;
      return { V.r ; w ; cleaned_up }

    let on_close self =
      let { V.r ; w ; cleaned_up } = !(state self) in
      log_event self Close_started >>= fun () ->
      Pipe.close w ;
      Pipe.close_read r ;
      cleaned_up >>= fun () ->
      log_event self Close_finished

    let on_launch self _ _ =
      init_connection self >>= fun conn ->
      let old_ts = ref Time_ns.min_value in
      Clock_ns.every'
        ~stop:conn.cleaned_up
        ~continue_on_error:false (Time_ns.Span.of_int_sec 60)
        begin fun () ->
          let evts = latest_events ~after:!old_ts self in
          old_ts := Time_ns.now () ;
          let evts = List.Assoc.find_exn
              ~equal:Pervasives.(=) evts Logs.Info in
          let nb_trades =
            Array.count evts ~f:begin fun (_, evt) ->
              match evt with E.Trade _ -> true | _ -> false
            end in
          let nb_bookentries =
            Array.count evts ~f:begin fun (_, evt) ->
              match evt with E.BookEntry _ -> true | _ -> false
            end in
          log_event self (E.TradesPerMinute nb_trades) >>= fun () ->
          log_event self (E.BookEntriesPerMinute nb_bookentries)
        end ;
      return (ref conn)

    let on_no_request self =
      (* Reinitialize dead connection *)
      log_event self Watchdog >>= fun () ->
      on_close self >>= fun () ->
      init_connection self >>= fun st ->
      (state self) := st ;
      Deferred.unit

    let on_completion _self _req _arg _status =
      Deferred.unit

    let on_error _self _view _status _error =
      Deferred.unit
  end
end

let heartbeat addr w ival =
  let ival = Option.value_map ival ~default:60 ~f:Int32.to_int_exn in
  let msg = DTC.default_heartbeat () in
  let rec loop () =
    Clock_ns.after @@ Time_ns.Span.of_int_sec ival >>= fun () ->
    match Connection.find addr with
    | None -> Deferred.unit
    | Some { Connection.dropped ; _ } ->
      Log_async.debug begin fun m ->
        m "-> [%a] Heartbeat" pp_print_addr addr
      end >>= fun () ->
      msg.num_dropped_messages <- Some (Int32.of_int_exn dropped) ;
      write_message w `heartbeat DTC.gen_heartbeat msg ;
      loop ()
  in
  loop ()

let encoding_request addr w =
  Log.debug
    (fun m -> m "<- [%a] Encoding Request" pp_print_addr addr) ;
  Encoding.(to_string (Response { version = 7 ; encoding = Protobuf })) |>
  Writer.write w ;
  Log.debug
    (fun m -> m "-> [%a] Encoding Response" pp_print_addr addr)

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
  Log.debug
    (fun m -> m "<- [%a] Logon Request" pp_print_addr addr) ;
  let accept trading =
    let trading_supported, result_text =
      match trading with
      | Ok msg -> true, Printf.sprintf "Trading enabled: %s" msg
      | Error msg -> false, Printf.sprintf "Trading disabled: %s" msg
    in
    don't_wait_for @@ heartbeat addr w req.heartbeat_interval_in_seconds;
    write_message w `logon_response
      DTC.gen_logon_response (logon_response ~trading_supported ~result_text) ;
    Log.debug begin fun m ->
      m "-> [%a] Logon Response (%s)" pp_print_addr addr result_text
    end ;
    if not !sc_mode || send_secdefs then begin
      String.Table.iteri tickers ~f:begin fun ~key:symbol ~data:_ ->
        let secdef = secdef_of_symbol ~final:true symbol in
        write_message w `security_definition_response
          DTC.gen_security_definition_response secdef ;
        Log.debug (fun m -> m "Written secdef %s" symbol)
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

let heartbeat _addr _msg =
  (* TODO: do something? *)
  (* Log.debug log_dtc "<- [%s] Heartbeat" addr *)
  ()

let security_definition_request log_evt addr w msg =
  let reject request_id symbol =
    Log.info begin fun m ->
      m "-> [%a] (req: %ld) Unknown symbol %s" pp_print_addr addr request_id symbol
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
    log_evt symbol ;
    Log.debug begin fun m ->
      m "<- [%a] Sec Def Request %ld %s %s"
        pp_print_addr addr request_id symbol exchange
    end ;
    if exchange <> my_exchange then reject request_id symbol
    else begin match String.Table.mem tickers symbol with
      | false -> reject request_id symbol
      | true ->
        let secdef = secdef_of_symbol ~final:true ~request_id symbol in
        Log.debug begin fun m ->
          m "-> [%a] Sec Def Response %ld %s %s"
            pp_print_addr addr request_id symbol exchange
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
    Log.debug begin fun m ->
      m "-> [%a] Market Data Reject: %s" pp_print_addr addr reject_text
    end ;
    write_message w `market_data_reject DTC.gen_market_data_reject rej
  end k

let write_market_data_snapshot ?id symbol w =
  let snap = DTC.default_market_data_snapshot () in
  snap.symbol_id <- id ;
  snap.session_high_price <- String.Table.find session_high symbol ;
  snap.session_low_price <- String.Table.find session_low symbol ;
  snap.session_volume <- String.Table.find session_volume symbol ;
  begin match String.Table.find latest_trades symbol with
    | None -> ()
    | Some { gid = _ ; id = _ ; ts; side = _ ; price; qty } ->
      snap.last_trade_price <- Some price ;
      snap.last_trade_volume <- Some qty ;
      snap.last_trade_date_time <- Some (Ptime.to_float_s ts) ;
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

let market_data_request log_evt addr w msg =
  let req = DTC.parse_market_data_request msg in
  let { Connection.subs ; rev_subs ; _ } = Connection.find_exn addr in
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
    Option.iter (Int32.Table.find rev_subs id) ~f:begin fun symbol ->
      log_evt `unsubscribe symbol ;
      String.Table.remove subs symbol
    end ;
    Int32.Table.remove rev_subs id
  | Some `snapshot, _, Some symbol, _ ->
    log_evt `snapshot symbol ;
    write_market_data_snapshot symbol w
  | Some `subscribe, Some id, Some symbol, _ ->
    log_evt `subscribe symbol ;
    begin
      match Int32.Table.find rev_subs id with
      | Some symbol' when symbol <> symbol' ->
        reject_market_data_request addr w ~id
          "Already subscribed to %s with a different id (was %ld)"
          symbol id
      | _ ->
        String.Table.set subs ~key:symbol ~data:id ;
        Int32.Table.set rev_subs ~key:id ~data:symbol ;
        write_market_data_snapshot ~id symbol w
    end
  | _ ->
    reject_market_data_request addr w "invalid request"

let write_market_depth_snapshot ?id addr w ~symbol ~num_levels =
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
  Log.debug begin fun m ->
    m "-> [%a] Market Depth Snapshot %s (%d/%d)"
      pp_print_addr addr symbol
      (Int.min bid_size num_levels) (Int.min ask_size num_levels)
  end

let reject_market_depth_request ?id addr w k =
  let rej = DTC.default_market_depth_reject () in
  rej.symbol_id <- id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    Log.debug begin fun m ->
      m "-> [%a] Market Depth Reject: %s"
        pp_print_addr addr reject_text
    end ;
    write_message w `market_depth_reject
      DTC.gen_market_depth_reject rej
  end k

let market_depth_request log_evt addr w msg =
  let req = DTC.parse_market_depth_request msg in
  let num_levels = Option.value_map req.num_levels ~default:50 ~f:Int32.to_int_exn in
  let { Connection.subs_depth ; rev_subs_depth ; _ } = Connection.find_exn addr in
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
    Option.iter (Int32.Table.find rev_subs_depth id) ~f:begin fun symbol ->
      log_evt `unsubscribe symbol ;
      String.Table.remove subs_depth symbol
    end ;
    Int32.Table.remove rev_subs_depth id
  | Some `subscribe, Some id, Some symbol, _ ->
    log_evt `subscribe symbol ;
    begin
      match Int32.Table.find rev_subs_depth id with
      | Some symbol' when symbol <> symbol' ->
        reject_market_depth_request addr w ~id
          "Already subscribed to %s with a different id (was %ld)"
          symbol id
      | _ ->
        String.Table.set subs_depth ~key:symbol ~data:id ;
        Int32.Table.set rev_subs_depth ~key:id ~data:symbol ;
        write_market_depth_snapshot ~id addr w ~symbol ~num_levels
    end
  | _ ->
    reject_market_depth_request addr w "invalid request"

let send_open_order_update w request_id nb_open_orders
    ~key:_ ~data:(symbol, { Rest.OpenOrder.id; side; price; qty; starting_qty; _ } ) i =
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
    let { Connection.orders ; _ } = Connection.find_exn addr in
    Log.debug begin fun m ->
      m "<- [%a] Open Orders Request" pp_print_addr addr
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
    Log.debug begin fun m ->
      m "-> [%a] %d order(s)" pp_print_addr addr nb_open_orders
    end
  | _ -> ()

let current_positions_request addr w msg =
  let { Connection.positions ; _ } = Connection.find_exn addr in
  Log.debug begin fun m ->
    m "<- [%a] Positions" pp_print_addr addr
  end ;
  let nb_msgs = String.Table.length positions in
  let req = DTC.parse_current_positions_request msg in
  let update = DTC.default_position_update () in
  let (_:Int32.t) =
    String.Table.fold positions
      ~init:1l ~f:begin fun ~key:symbol ~data:{ price; qty ; _ } msg_number ->
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
  Log.debug begin fun m ->
    m "-> [%a] %d position(s)" pp_print_addr addr nb_msgs
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
    { Rest.TradeHistory.gid; id = _; ts; price; qty; fee = _; order_id = _; side; category = _ } =
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
  let { Connection.trades ; _ } = Connection.find_exn addr in
  let req = DTC.parse_historical_order_fills_request msg in
  let resp = DTC.default_historical_order_fill_response () in
  let min_ts =
    Option.value_map req.number_of_days ~default:Time_ns.epoch ~f:begin fun n ->
      Time_ns.(sub (now ()) (Span.of_day (Int32.to_float n)))
    end in
  Log.debug begin fun m ->
    m "<- [%a] Historical Order Fills Req" pp_print_addr addr
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
            match snd a, (Rest.TradeHistory.Set.find data ~f:(fun { gid ; _ } -> gid = srv_ord_id)) with
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
  Log.debug begin fun m ->
    m "<- [%a] TradeAccountsRequest" pp_print_addr addr
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
    Log.debug begin fun m ->
      m "-> [%a] TradeAccountResponse: %s (%ld/%d)"
        pp_print_addr addr trade_account msg_number nb_msgs
    end
  end

let reject_account_balance_request addr request_id account =
  let rej = DTC.default_account_balance_reject () in
  rej.request_id <- request_id ;
  rej.reject_text <- Some ("Unknown account " ^ account) ;
  Log.debug begin fun m ->
    m "-> [%a] AccountBalanceReject: unknown account %s"
      pp_print_addr addr account
  end

let account_balance_request addr msg =
  let req = DTC.parse_account_balance_request msg in
  let c = Connection.find_exn addr in
  match req.trade_account with
  | None
  | Some "" ->
    Log.debug begin fun m ->
      m "<- [%a] AccountBalanceRequest (all accounts)" pp_print_addr c.addr
    end ;
    Connection.write_exchange_balance ?request_id:req.request_id ~msg_number:1 ~nb_msgs:2 c;
    Connection.write_margin_balance ?request_id:req.request_id ~msg_number:2 ~nb_msgs:2 c
  | Some account when account = exchange_account ->
    Log.debug begin fun m ->
      m "<- [%a] AccountBalanceRequest (%s)" pp_print_addr c.addr account
    end ;
    Connection.write_exchange_balance ?request_id:req.request_id c
  | Some account when account = margin_account ->
    Log.debug begin fun m ->
      m "<- [%a] AccountBalanceRequest (%s)" pp_print_addr c.addr account
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
let submit_new_order
    ({ Connection.addr ; w ; key ; secret ; client_orders ; orders ; _ } as conn)
    (req : DTC.submit_new_single_order) =
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
  Log.debug begin fun m ->
    m "-> [%a] Submit Order %s %f %f"
      pp_print_addr addr symbol price qty
  end ;
  order_f ~buf:buf_json ?tif ~key ~secret ~side ~symbol ~price ~qty () >>| function
  | Error Rest.Http_error.Poloniex msg ->
    reject_new_order w req "%s" msg
  | Error err ->
    Option.iter req.client_order_id ~f:begin fun id ->
      reject_new_order w req "%s: %s" id (Rest.Http_error.to_string err)
    end
  | Ok { id; trades; amount_unfilled } -> begin
      Int.Table.set client_orders ~key:id ~data:req ;
      Int.Table.set orders ~key:id
        ~data:(symbol, open_order_of_submit_new_single_order id req margin) ;
      Log.debug begin fun m ->
        m "<- [%a] Submit Order OK %d" pp_print_addr addr id
      end ;
      match trades, amount_unfilled with
      | [], _ ->
        send_new_order_update w req
          ~status:`order_status_open
          ~reason:`new_order_accepted
          ~server_order_id:id
          ~filled_qty:0.
          ~remaining_qty:qty
      | _trades, 0. ->
        send_new_order_update w req
          ~status:`order_status_filled
          ~reason:`order_filled
          ~server_order_id:id
          ~filled_qty:qty
          ~remaining_qty:0. ;
        if margin then
          RestSync.Default.push_nowait
            (fun () -> Connection.update_positions conn)
      | trades, _unfilled ->
        let trades = Rest.OrderResponse.trades_of_symbol trades symbol in
        let filled_qty =
          List.fold_left trades ~init:0. ~f:(fun a { qty ; _ } -> a +. qty) in
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

let submit_new_single_order
    ({ Connection.w ; _ } as conn)
    (req : DTC.submit_new_single_order) =
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
        Option.(req.symbol >>= String.Table.find latest_trades >>| fun { price ; _ } -> price *. 2.)
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

let submit_new_single_order addr msg =
  let conn = Connection.find_exn addr in
  let req = DTC.parse_submit_new_single_order msg in
  Log.debug begin fun m ->
    m "<- [%a] Submit New Single Order" pp_print_addr conn.addr
  end ;
  try submit_new_single_order conn req with
  | Exit -> ()
  | exn -> Log.err (fun m -> m "%a" Exn.pp exn)

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

let cancel_order addr msg =
  let { Connection.w ; key ; secret ; client_orders ; orders ; _} =
    Connection.find_exn addr in
  let req = DTC.parse_cancel_order msg in
  match Option.map req.server_order_id ~f:Int.of_string with
  | None ->
    reject_cancel_order w req "Server order id not set"
  | Some order_id ->
    Log.debug begin fun m ->
      m "<- [%a] Order Cancel %d" pp_print_addr addr order_id
    end ;
    RestSync.Default.push_nowait begin fun () ->
      Rest.cancel_order ~key ~secret ~order_id () >>| function
      | Error Rest.Http_error.Poloniex msg ->
        reject_cancel_order w req "%s" msg
      | Error _ ->
        reject_cancel_order w req
          "exception raised while trying to cancel %d" order_id
      | Ok () ->
        Log.debug begin fun m ->
          m "-> [%a] Order Cancel OK %d" pp_print_addr addr order_id
        end ;
        let order_id_str = Int.to_string order_id in
        match Int.Table.find client_orders order_id,
              Int.Table.find orders order_id with
        | None, None ->
          Log.err begin fun m ->
            m "<- [%a] Unable to find order id %d in tables"
              pp_print_addr addr order_id
          end ;
          send_cancel_update w order_id_str
            (DTC.default_submit_new_single_order ())
        | Some client_order, _ ->
          Int.Table.remove orders order_id ;
          send_cancel_update w order_id_str client_order ;
        | None, Some (symbol, order) ->
          Log.err begin fun m ->
            m "[%a] Found open order %d but no matching client order"
              pp_print_addr addr order_id
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
    Log.debug begin fun m ->
      m "-> [%a] Cancel Replace Reject: %s" pp_print_addr addr info_text
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

let cancel_replace_order addr msg =
  let { Connection.addr ; w ; key ; secret ; client_orders ; orders ; _ }
    = Connection.find_exn addr in
  let req = DTC.parse_cancel_replace_order msg in
  Log.debug begin fun m ->
    m "<- [%a] Cancel Replace Order" pp_print_addr addr
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
        | Ok { id; trades = _ ; amount_unfilled } ->
          Log.debug begin fun m ->
            m "<- [%a] Cancel Replace Order %d -> %d OK"
              pp_print_addr addr orig_server_id id
          end ;
          let order_id_str = Int.to_string id in
          let amount_unfilled = amount_unfilled *. 1e4 in
          match Int.Table.find client_orders orig_server_id,
                Int.Table.find orders orig_server_id with
          | None, None ->
            Log.err begin fun m ->
              m "[%a] Unable to find order id %d in tables"
                pp_print_addr addr orig_server_id
            end ;
            send_cancel_replace_update w order_id_str amount_unfilled
              (DTC.default_submit_new_single_order ()) req
          | Some client_order, Some (symbol, open_order) ->
            Int.Table.remove client_orders orig_server_id ;
            Int.Table.remove orders orig_server_id ;
            Int.Table.set client_orders ~key:id ~data:client_order ;
            Int.Table.set orders ~key:id ~data:(symbol, { open_order with qty = amount_unfilled }) ;
            send_cancel_replace_update
              w order_id_str amount_unfilled client_order req
          | Some client_order, None ->
            Log.err begin fun m ->
              m "[%a] Found client order %d but no matching open order"
                pp_print_addr addr orig_server_id
            end ;
            Int.Table.remove client_orders orig_server_id ;
            Int.Table.set client_orders ~key:id ~data:client_order ;
            send_cancel_replace_update
              w order_id_str amount_unfilled client_order req
          | None, Some (symbol, order) ->
            Log.err begin fun m ->
              m "[%a] Found open order %d but no matching client order"
                pp_print_addr addr orig_server_id
            end ;
            send_cancel_replace_update w order_id_str amount_unfilled
              (submit_new_single_order_of_open_order symbol order) req
      end

module Actor_dtc = struct
  module N = struct
    let base = ["dtc"]
    type t = string
    let pp = Format.pp_print_string
    let to_string t = Format.asprintf "%a" pp t
  end
  module E = struct
    type evt =
      | TCP_handler_error of Exn.t
      | Connection_io_error of Exn.t

      | Connect

      | Encoding_request
      | Logon_request
      | Logoff
      | Heartbeat
      | SecurityDefinitionForSymbolRequest of string
      | MarketDataRequest of { action : [`subscribe | `unsubscribe | `snapshot] ;
                               sym : string }
      | MarketDepthRequest of { action : [`subscribe | `unsubscribe | `snapshot] ;
                                sym : string }
    [@@deriving sexp_of]

    type t = {
      ts: Time_ns.t ;
      src: Socket.Address.Inet.t ;
      evt: evt ;
    } [@@deriving sexp_of]

    let http_port =
      Option.map ~f:Int.of_string (Sys.getenv "PLNX_DTC_HTTP_PORT")

    let warp10_url = None
    let to_warp10 _ = None

    let create src evt = { src ; evt }
    let dummy = create
        (Socket.Address.Inet.create Unix.Inet_addr.localhost ~port:0) Connect
    let level { evt ; _ } =
      match evt with
      | TCP_handler_error _
      | Connection_io_error _ -> Logs.Error
      | _ -> Info
    let pp ppf v= Sexplib.Sexp.pp ppf (sexp_of_t v)
  end
  module R = struct
    type 'a t = {
      ret: 'a ;
    }
    type view = unit [@@deriving sexp]
    let view { ret = _ } = ()
    let pp ppf v = Sexplib.Sexp.pp ppf (sexp_of_view v)
  end
  module V = struct
    type state_ = (Socket.Address.Inet.t, int) Tcp.Server.t
    type state = state_ ref
    type parameters = {
      server : Conduit_async.server ;
      port : int
    }
    type view = unit
    let view _ _ = ()
    let pp ppf _ = Format.pp_print_string ppf ""
  end
  module A = Actor.Make(N)(E)(R)(V)
  include A

  let server_fun self addr r w =
    let on_connection_io_error exn =
      log_event_now self (E.create addr (Connection_io_error exn)) ;
      Connection.remove addr
    in
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
            | `logoff ->
              let _ = DTC.parse_logoff msg in
              write_message w `logoff DTC.gen_logoff
                { DTC.Logoff.reason = Some "bye" ;
                  do_not_reconnect = Some false }  ;
              log_event_now self (E.create addr Logoff) ;
            | `encoding_request ->
              log_event_now self (E.create addr Logon_request) ;
              begin match (Encoding.read (Bigstring.To_string.subo buf ~pos ~len:16)) with
                | None -> Log.err begin fun m ->
                    m "Invalid encoding request received"
                  end
                | Some _ -> encoding_request addr w
              end
            | `logon_request ->
              log_event_now self (E.create addr Logon_request) ;
              logon_request addr w msg
            | `heartbeat ->
              log_event_now self (E.create addr Heartbeat) ;
              heartbeat addr msg
            | `security_definition_for_symbol_request ->
              let log_evt sym = log_event_now self
                  (E.create addr (SecurityDefinitionForSymbolRequest sym)) in
              security_definition_request log_evt addr w msg
            | `market_data_request ->
              let log_evt action sym = log_event_now self
                  (E.create addr (MarketDataRequest { action ; sym })) in
              market_data_request log_evt addr w msg
            | `market_depth_request ->
              let log_evt action sym = log_event_now self
                  (E.create addr (MarketDepthRequest { action ; sym })) in
              market_depth_request log_evt addr w msg
            | `open_orders_request -> open_orders_request addr w msg
            | `current_positions_request -> current_positions_request addr w msg
            | `historical_order_fills_request -> historical_order_fills addr w msg
            | `trade_accounts_request -> trade_account_request addr w msg
            | `account_balance_request -> account_balance_request addr msg
            | `submit_new_single_order -> submit_new_single_order addr msg
            | `cancel_order -> cancel_order addr msg
            | `cancel_replace_order -> cancel_replace_order addr msg
            | #DTC.dtcmessage_type ->
              Log.err begin fun m -> m
                  "Unknown msg type %d" msgtype_int
              end
          end ;
          handle_chunk (consumed + msglen) buf ~pos:(pos + msglen) ~len:(len - msglen)
        end in
    Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_connection_io_error;
    log_event self (E.create addr Connect) >>= fun () ->
    Reader.read_one_chunk_at_a_time r
      ~handle_chunk:(handle_chunk 0) >>= fun _ ->
    (* TODO: handle error *)
    Deferred.unit

  module Handlers : HANDLERS
    with type self = bounded queue t = struct

    type self = bounded queue t

    let on_handler_error self addr exn =
      log_event_now self (E.create addr (TCP_handler_error exn))

    let on_request _self { R.ret } =
      return ret

    let on_close _self =
      Deferred.unit

    let on_launch self _name { V.server ; port } =
      Conduit_async.serve
        ~on_handler_error:(`Call (on_handler_error self))
        server (Tcp.Where_to_listen.of_port port)
        (server_fun self) >>| fun srv ->
      ref srv

    let on_no_request _self =
      Deferred.unit

    let on_completion _self _req _arg _status =
      Deferred.unit

    let on_error _self _view _status _error =
      Deferred.unit
  end
end

let main span timeout tls uid gid port sc () =
  sc_mode := sc ;
  update_client_span := span ;
  stage begin fun `Scheduler_started ->
    let logs =
      Option.Monad_infix.(Sys.getenv "OVH_LOGS_URL" >>| Uri.of_string) in
    Logs_async_ovh.udp_reporter ?logs () >>= fun reporter ->
    Logs.set_reporter reporter ;
    let now = Time_ns.now () in
    Rest.currencies () >>| begin function
      | Error err -> failwithf "currencies: %s" (Rest.Http_error.to_string err) ()
      | Ok currs ->
        List.iter currs ~f:(fun (c, t) -> String.Table.set currencies ~key:c ~data:t)
    end >>= fun () ->
    Rest.tickers () >>| begin function
      | Error err -> failwithf "tickers: %s" (Rest.Http_error.to_string err) ()
      | Ok ts ->
        List.iter ts ~f:begin fun (key, t) ->
          String.Table.set tickers ~key ~data:(now, t)
        end
    end >>= fun () ->
    RestSync.Default.run () ;
    conduit_server ?tls () >>= fun server ->
    Option.iter uid ~f:Core.Unix.setuid ;
    Option.iter gid ~f:Core.Unix.setgid ;
    Actor_ws.launch
      (Actor_ws.bounded 10) ~timeout
      (Actor.Types.create_limits
         ?backlog_level:(Logs.Src.level src)
         ~backlog_size:100_000
         ()) "main" ()
      (module Actor_ws.Handlers) >>= fun _ws_worker ->
    Actor_dtc.launch
      (Actor_dtc.bounded 10)
      (Actor.Types.create_limits
         ?backlog_level:(Logs.Src.level src)
         ~backlog_size:50_000
         ()) "main" { server ; port }
      (module Actor_dtc.Handlers) >>= fun _dtc_worker ->
    Deferred.never ()
  end

let () =
  let open Bs_devkit.Arg_type in
  let open Command.Let_syntax in
  Command.Staged.async ~summary:"Poloniex bridge"
    [%map_open
      let client_span =
        flag_optional_with_default_doc "update-client-span"
          span Time_ns.Span.sexp_of_t
          ~default:(Time_ns.Span.of_int_sec 30)
          ~doc:"span Span between client updates"
      and timeout =
        flag_optional_with_default_doc "timeout"
          span Time_ns.Span.sexp_of_t
          ~default:(Time_ns.Span.of_int_sec 10)
          ~doc:"span Max Disconnect if no message received in N seconds"
      and port =
        flag_optional_with_default_doc "port"
          int sexp_of_int ~default:5573 ~doc:"int TCP port to use"
      and crt =
        flag "crt-file" (optional string)
          ~doc:"filename crt file to use (TLS)"
      and key =
        flag "key-file" (optional string)
          ~doc:"filename key file to use (TLS)"
      and sc =
        flag "sc" no_arg ~doc:" Sierra Chart mode"
      and uid =
        flag "setuid" (optional int) ~doc:" Set uid after reading TLS file"
      and gid =
        flag "setgid" (optional int) ~doc:" Set gid after reading TLS file"
      and () =
        Logs_async_reporter.set_level_via_param None in
      fun () ->
        let tls =
          match crt, key with
          | Some crt, Some key -> Some (crt, key)
          | _ -> None in
        main client_span timeout tls uid gid port sc ()
    ]
  |> Command.run
