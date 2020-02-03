open Core
open Async

open Plnx
open Bmex_common
open Poloniex_util
open Poloniex_global
module Conn = Poloniex_connection

module Ws = Plnx_ws

let at_bid_or_ask_of_depth : Side.t -> DTC.at_bid_or_ask_enum = function
  | Buy -> `at_bid
  | Sell -> `at_ask

let send_depth_update
    (update : DTC.Market_depth_update_level.t)
    w side (u : Plnx.BookEntry.t) =
  let update_type =
    if Float.equal u.qty 0.
    then `market_depth_delete_level
    else `market_depth_insert_update_level in
  update.side <- Some (at_bid_or_ask_of_depth side) ;
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

let on_book_update pair ts side ({ Plnx.BookEntry.price; qty } as u) =
  let old_bids = (Book.get_bids pair).book in
  let old_asks = (Book.get_asks pair).book in
  let old_best_bid = Option.value_map ~f:fst ~default:Float.min_value (Float.Map.max_elt old_bids) in
  let old_best_ask = Option.value_map ~f:fst ~default:Float.max_value (Float.Map.min_elt old_asks) in
  let _book, new_book = match side with
    | Fixtypes.Side.Buy ->
      let { Book.book ; _ } = Book.get_bids pair in
      let new_book =
        Float.(if qty > 0. then Map.set book ~key:price ~data:qty
         else Map.remove book price) in
      Book.set_bids ~symbol:pair ~ts ~book:new_book ;
      book, new_book
    | Sell ->
      let { Book.book ; _ } = Book.get_asks pair in
      let new_book =
        Float.(if qty > 0. then Map.set book ~key:price ~data:qty
               else Map.remove book price) in
      Book.set_asks ~symbol:pair ~ts ~book:new_book ;
      book, new_book
  in
  let new_best_bidask = match side with
    | Fixtypes.Side.Buy ->
      let new_best_bid =
        Option.value_map ~f:fst ~default:Float.min_value (Float.Map.max_elt new_book) in
      if Float.(new_best_bid > old_best_bid) then
        Some (new_best_bid, old_best_ask) else None
    | Sell ->
      let new_best_ask =
        Option.value_map ~f:fst ~default:Float.max_value (Float.Map.min_elt new_book) in
      if Float.(new_best_ask < old_best_ask) then
        Some (old_best_bid, new_best_ask) else None
  in
  let on_connection { Conn.addr; w; subs; subs_depth ; _ } =
    let update_depth symbol_id =
      depth_update.symbol_id <- Some symbol_id ;
      Log.debug begin fun m ->
        m "-> [%a] %a D %a"
          pp_print_addr addr Pair.pp pair Sexp.pp_hum (Plnx.BookEntry.sexp_of_t u)
      end ;
      send_depth_update depth_update w side u
    in
    let update_bidask symbol_id (bid, ask) =
      bidask_update.symbol_id <- Some symbol_id ;
      Log.debug begin fun m ->
        m "-> [%a] %a BIDASK %a"
          pp_print_addr addr Pair.pp pair Sexp.pp_hum (Plnx.BookEntry.sexp_of_t u)
      end ;
      send_bidask_update bidask_update w bid ask
    in
    match Pair.Table.(find_opt subs pair, find_opt subs_depth pair) with
    | _, Some symbol_id -> update_depth symbol_id
    | Some symbol_id, _ -> Option.iter new_best_bidask ~f:(update_bidask symbol_id)
    | _ -> ()
  in
  Conn.iter ~f:on_connection

let at_bid_or_ask_of_trade : Side.t -> DTC.at_bid_or_ask_enum = function
  | Fixtypes.Side.Buy -> `at_ask
  | Sell -> `at_bid

let on_trade_update pair ({ Trade.ts; side; price; qty; _ } as t) =
  Pair.Table.add latest_trades pair t ;
  let session_high =
    let h =
      Option.value ~default:Float.min_value (Pair.Table.find_opt session_high pair) in
    if Float.(h < t.price) then begin
      Pair.Table.add session_high pair t.price ;
      Some t.price
    end else None in
  let session_low =
    let l = Option.value ~default:Float.max_value (Pair.Table.find_opt session_low pair) in
    if Float.(l > t.price) then begin
      Pair.Table.add session_low pair t.price ;
      Some t.price
    end else None in
  Pair.Table.add session_volume pair begin
    match (Pair.Table.find_opt session_volume pair) with
    | None -> t.qty
    | Some qty -> qty +. t.qty
  end ;
  Log.debug begin fun m ->
    m "<- %a %a" Pair.pp pair Sexp.pp_hum (Trade.sexp_of_t t)
  end ;
  (* Send trade updates to subscribers. *)
  let on_connection { Conn.w ; subs ; _ } =
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
    Option.iter Pair.Table.(find_opt subs pair) ~f:on_symbol_id
  in
  Conn.iter ~f:on_connection

module E = struct
  type t =
    | Connect
    | Watchdog
    | Close_started
    | Close_finished
    | Snapshot of Pair.t
    | Trade of Pair.t
    | BookEntry of Side.t * Pair.t

    | TradesPerMinute of int
    | BookEntriesPerMinute of int
  [@@deriving sexp]

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
  let level _ = Logs.Debug
  let pp ppf v = Sexplib.Sexp.pp ppf (sexp_of_view v)
end
module V = struct
  type state = {
    buf : Bi_outbuf.t ;
    mutable feed : Plnx_ws.t Pipe.Reader.t ;
  }
  type parameters = unit
  type view = unit
  let view _ () = ()
  let pp ppf _ = Format.pp_print_string ppf ""
end
module A = Actor.Make(E)(R)(V)
include A

module Handlers : HANDLERS
  with type self = bounded queue t = struct
  type self = bounded queue t

  let on_launch_complete _ =
    Deferred.unit

  let on_request self { R.ts ; evt = { chanid; seqnum = _; events } ; ret } =
    List.iter events ~f:begin function
      | Ws.Snapshot { symbol ; bid ; ask } ->
        record_event self (Snapshot symbol) ;
        Int.Table.set subid_to_sym ~key:chanid ~data:symbol ;
        Book.set_bids ~symbol ~ts ~book:bid ;
        Book.set_asks ~symbol ~ts ~book:ask ;
      | BookEntry (side, entry) ->
        let pair = Int.Table.find_exn subid_to_sym chanid in
        record_event self (BookEntry (side, pair)) ;
        on_book_update pair ts side entry
      | Trade t ->
        let pair = Int.Table.find_exn subid_to_sym chanid in
        record_event self (Trade pair) ;
        on_trade_update pair t
      | Ticker _ -> ()
      | Err _ -> ()
    end ;
    return ret

  let init_connection ?buf self =
    let buf =
      match status self, buf with
      | Launching _, None -> invalid_arg "init_connection"
      | Launching _, Some buf -> buf
      | _, _ -> (state self).buf in
    let pairs = Pair.Table.fold (fun k _v a -> k::a) tickers [] in
    (* TODO: persistent connection *)
    Fastws_async.connect
      ~of_string:(Ws.of_string ~buf)
      ~to_string:(Ws.string_of_command ~buf)
      Plnx_ws.url >>= fun {r; w } ->
    log_event self Connect >>= fun () ->
    Deferred.List.iter pairs ~f:begin fun pair ->
      Pipe.write w (Ws.Subscribe (Plnx_ws.TradesQuotes pair))
    end >>= fun () ->
    Pipe.close w ;
    let push_request evt =
      let ts = Time_ns.now () in
      push_request self { ts ; evt ; ret = () } in
    don't_wait_for (Pipe.iter r ~f:push_request) ;
    return r

  let on_close self =
    let r = (state self).feed in
    log_event self Close_started >>= fun () ->
    Pipe.close_read r ;
    (Pipe.closed r >>> fun () -> log_event_now self Close_finished) ;
    Deferred.unit

  let on_launch self _ _ =
    let buf = Bi_outbuf.create 4096 in
    init_connection ~buf self >>= fun feed ->
    let old_ts = ref Time_ns.epoch in
    Clock_ns.every'
      ~stop:(Pipe.closed feed)
      ~continue_on_error:false (Time_ns.Span.of_int_sec 60)
      begin fun () ->
        let evts = latest_events ~after:!old_ts self in
        old_ts := Time_ns.now () ;
        let evts = List.Assoc.find_exn
            ~equal:Stdlib.(=) evts Logs.Info in
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
    return { V.buf ; feed }

  let on_no_request self =
    (* Reinitialize dead connection *)
    log_event self Watchdog >>= fun () ->
    on_close self >>= fun () ->
    init_connection self >>= fun feed ->
    let s = state self in
    s.feed <- feed ;
    Deferred.unit

  let on_completion _self _req _arg _status =
    Deferred.unit

  let on_error _self _view _status _error =
    Deferred.unit
end
