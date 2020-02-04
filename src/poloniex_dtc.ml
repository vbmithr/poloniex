open Core
open Async

open Plnx
open Bmex_common
open Poloniex_util
open Poloniex_global
module Conn = Poloniex_connection

module Encoding = Dtc_pb.Encoding
module DTC = Dtc_pb.Dtcprotocol_piqi

module E = struct
  type trade_mode_enum =
    [ `trade_mode_demo
    | `trade_mode_live
    | `trade_mode_simulated
    | `trade_mode_unset ] [@@deriving sexp]

  type logon_request = DTC.Logon_request.t = {
    mutable protocol_version : int32 option;
    mutable username : string option;
    mutable password : string option [@opaque] ;
    mutable general_text_data : string option;
    mutable integer_1 : int32 option;
    mutable integer_2 : int32 option;
    mutable heartbeat_interval_in_seconds : int32 option;
    mutable trade_mode : trade_mode_enum option;
    mutable trade_account : string option;
    mutable hardware_identifier : string option;
    mutable client_name : string option;
  } [@@deriving sexp]

  type evt =
    | TCP_handler_error of Exn.t
    | Connection_io_error of Exn.t

    | Connect

    | Encoding_request
    | Logon_request of logon_request
    | Logoff
    | Heartbeat_sent
    | Heartbeat_recv
    | SecurityDefinitionForSymbolRequest of string
    | MarketDataRequest of
        { action: [`subscribe | `unsubscribe | `snapshot] ; pair: Pair.t }
    | MarketDepthRequest of
        { action: [`subscribe | `unsubscribe | `snapshot] ; pair: Pair.t }

    | NbConnected of int
  [@@deriving sexp_of]

  type t = {
    src: Socket.Address.Inet.t ;
    evt: evt ;
  } [@@deriving sexp_of]

  let http_port =
    Option.map ~f:Int.of_string (Sys.getenv "PLNX_DTC_HTTP_PORT")

  let create src evt = { src ; evt }
  let nb_connected src i = { src ; evt = NbConnected i }
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
  let level _ = Logs.Debug
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
module A = Actor.Make(E)(R)(V)
include A

let encoding_request addr w =
  Log.debug
    (fun m -> m "<- [%a] Encoding Request" pp_print_addr addr) ;
  Writer.write w
    Encoding.(to_string (Response { version = 8 ; encoding = Protobuf })) ;
  Log.debug
    (fun m -> m "-> [%a] Encoding Response" pp_print_addr addr)

let logon_response ~result ~result_text ~trading_supported =
  let resp = DTC.default_logon_response () in
  resp.server_name <- Some "Poloniex" ;
  resp.protocol_version <- Some 8l ;
  resp.result <- Some result ;
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

let descr_of_symbol ({ base; quote }:Pair.t) =
  let buf = Buffer.create 32 in
  let quote = String.Table.find_exn currencies quote in
  let base = String.Table.find_exn currencies base in
  Buffer.add_string buf base.name;
  Buffer.add_string buf " / ";
  Buffer.add_string buf quote.name;
  Buffer.contents buf

let secdef_of_pair ?request_id ?(final=true) pair =
  let request_id = match request_id with
    | Some reqid -> reqid
    | None when !sc_mode -> 110_000_000l
    | None -> 0l in
  let secdef = DTC.default_security_definition_response () in
  secdef.request_id <- Some request_id ;
  secdef.is_final_message <- Some final ;
  secdef.symbol <- Some (Pair.to_string pair) ;
  secdef.exchange <- Some my_exchange ;
  secdef.security_type <- Some `security_type_forex ;
  secdef.description <- Some (descr_of_symbol pair) ;
  secdef.min_price_increment <- Some 1e-8 ;
  secdef.currency_value_per_increment <- Some 1e-8 ;
  secdef.price_display_format <- Some `price_display_format_decimal_8 ;
  secdef.has_market_depth_data <- Some true ;
  secdef

let logon_accept w addr send_secdefs trading =
  let trading_supported, result_text =
    match trading with
    | Ok msg -> true, Printf.sprintf "Trading enabled: %s" msg
    | Error msg -> false, Printf.sprintf "Trading disabled: %s" msg
  in
  write_message w `logon_response DTC.gen_logon_response
    (logon_response ~result:`logon_success ~trading_supported ~result_text) ;
  Log.debug begin fun m ->
    m "-> [%a] Logon Response (%s)" pp_print_addr addr result_text
  end ;
  if not !sc_mode || send_secdefs then begin
    Pair.Table.iter begin fun pair _ ->
      let secdef = secdef_of_pair ~final:true pair in
      write_message w `security_definition_response
        DTC.gen_security_definition_response secdef ;
      Log.debug (fun m -> m "Written secdef %a" Pair.pp pair)
    end tickers
  end

let logon_reject addr w result_text =
  write_message w `logon_response DTC.gen_logon_response
    (logon_response ~result:`logon_error ~trading_supported:false ~result_text) ;
  Log.debug begin fun m ->
    m "-> [%a] Logon Response (%s)" pp_print_addr addr result_text
  end

let logon_request self addr w req =
  let int1 = Option.value ~default:0l req.DTC.Logon_request.integer_1 in
  let send_secdefs = Int32.(bit_and int1 128l <> 0l) in
  match req.heartbeat_interval_in_seconds with
  | None ->
    logon_reject addr w "Heartbeat not set"
  | Some hb_interval ->
    let hb_interval = Int32.to_int_exn hb_interval in
    let log_evt () =
      log_event_now self (E.create addr Heartbeat_sent) in
    (* let key = Option.value ~default:"" req.username in
     * let secret = Option.value ~default:"" req.password in
     * match key, secret with
     * | "", _ | _, "" -> *)
    Log.debug
      (fun m -> m "<- [%a] Anonymous Logon Request" pp_print_addr addr) ;
    let (_: Conn.t) = Conn.setup
        ~log_evt ~addr ~w ~key:"" ~secret:"" ~send_secdefs ~hb_interval in
    record_event self (E.nb_connected addr (Conn.length ())) ;
    logon_accept w addr send_secdefs (Result.fail "No credentials")
(* | _ ->
 *   Log.debug begin fun m ->
 *     m "<- [%a] User Logon Request (%s)" pp_print_addr addr key
 *   end ;
 *   let conn = Connection.setup
 *       ~log_evt ~addr ~w ~key  ~secret ~send_secdefs ~hb_interval in
 *   record_event self (E.nb_connected addr (Connection.length ())) ;
 *   Restsync.Default.push_nowait begin fun () ->
 *     Connection.setup_trading ~key ~secret conn >>| function
 *     | true ->
 *       logon_accept w addr send_secdefs
 *         (Result.return "Valid Poloniex credentials")
 *     | false ->
 *       logon_accept w addr send_secdefs
 *         (Result.fail "Invalid Poloniex crendentials")
 *   end *)

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
  | Some request_id, Some symbol, Some exchange -> begin
      match Pair.of_string symbol with
      | None -> reject request_id symbol
      | Some pair ->
        log_evt symbol ;
        Log.debug begin fun m ->
          m "<- [%a] Sec Def Request %ld %a %s"
            pp_print_addr addr request_id Pair.pp pair exchange
        end ;
        if not (String.equal exchange my_exchange) then reject request_id symbol
        else begin match Pair.Table.mem tickers pair with
          | false -> reject request_id symbol
          | true ->
            let secdef = secdef_of_pair ~final:true ~request_id pair in
            Log.debug begin fun m ->
              m "-> [%a] Sec Def Response %ld %s %s"
                pp_print_addr addr request_id symbol exchange
            end ;
            write_message w `security_definition_response
              DTC.gen_security_definition_response secdef
        end
    end
  | _ -> ()

let reject_market_data_request ?id addr w k =
  let rej = DTC.default_market_data_reject () in
  rej.symbol_id <- id ;
  Format.kasprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    Log.debug begin fun m ->
      m "-> [%a] Market Data Reject: %s" pp_print_addr addr reject_text
    end ;
    write_message w `market_data_reject DTC.gen_market_data_reject rej
  end k

let write_market_data_snapshot ?id pair w =
  let snap = DTC.default_market_data_snapshot () in
  snap.symbol_id <- id ;
  snap.session_high_price <- Pair.Table.find_opt session_high pair ;
  snap.session_low_price <- Pair.Table.find_opt session_low pair ;
  snap.session_volume <- Pair.Table.find_opt session_volume pair ;
  begin match Pair.Table.find_opt latest_trades pair with
    | None -> ()
    | Some { ts; price; qty; _ } ->
      snap.last_trade_price <- Some price ;
      snap.last_trade_volume <- Some qty ;
      snap.last_trade_date_time <- Some (Ptime.to_float_s ts) ;
  end ;
  let bid = Book.get_bids pair in
  let ask = Book.get_asks pair in
  let ts = Time_ns.max bid.ts ask.ts in
  if Time_ns.is_later ts ~than:Time_ns.epoch then
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
  let { Conn.subs ; rev_subs ; _ } = Conn.find_exn addr in
  match req.request_action,
        req.symbol_id,
        Option.map ~f:Pair.of_string_opt req.symbol,
        req.exchange
  with
  | _,_, Some (Error symbol), _ ->
    reject_market_data_request addr w "Wrongly formatted pair %s" symbol
  | _, id, _, Some exchange when not (String.equal exchange my_exchange) ->
    reject_market_data_request ?id addr w "No such exchange %s" exchange
  | _, id, Some (Ok pair), _ when not (Pair.Table.mem tickers pair) ->
    reject_market_data_request ?id addr w "No such symbol %a" Pair.pp pair
  | Some `unsubscribe, Some id, _, _ ->
    Option.iter (Int32.Table.find rev_subs id) ~f:begin fun pair ->
      log_evt `unsubscribe pair ;
      Pair.Table.remove subs pair
    end ;
    Int32.Table.remove rev_subs id
  | Some `snapshot, _, Some (Ok pair), _ ->
    log_evt `snapshot pair ;
    write_market_data_snapshot pair w
  | Some `subscribe, Some id, Some (Ok pair), _ ->
    log_evt `subscribe pair ;
    begin
      match Int32.Table.find rev_subs id with
      | Some p' when not (Pair.equal pair p') ->
        reject_market_data_request addr w ~id
          "id %ld already used for pair %a" id Pair.pp pair
      | _ ->
        Pair.Table.add subs pair id ;
        Int32.Table.set rev_subs ~key:id ~data:pair ;
        write_market_data_snapshot ~id pair w
    end
  | _ ->
    reject_market_data_request addr w "invalid request"

let write_market_depth_snapshot ?id addr w ~pair ~num_levels =
  let bid = Book.get_bids pair in
  let ask = Book.get_asks pair in
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
    m "-> [%a] Market Depth Snapshot %a (%d/%d)"
      pp_print_addr addr Pair.pp pair
      (Int.min bid_size num_levels) (Int.min ask_size num_levels)
  end

let reject_market_depth_request ?id addr w k =
  let rej = DTC.default_market_depth_reject () in
  rej.symbol_id <- id ;
  Format.kasprintf begin fun reject_text ->
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
  let { Conn.subs_depth ; rev_subs_depth ; _ } = Conn.find_exn addr in
  match req.request_action,
        req.symbol_id,
        Option.map ~f:Pair.of_string_opt req.symbol,
        req.exchange
  with
  | _, _, Some (Error symbol), _ ->
    reject_market_data_request addr w "Wrongly formatted pair %s" symbol
  | _, id, _, Some exchange when not (String.equal exchange my_exchange) ->
    reject_market_depth_request ?id addr w "No such exchange %s" exchange
  | _, id, Some (Ok pair), _ when not (Pair.Table.mem tickers pair) ->
    reject_market_depth_request ?id addr w "No such symbol %a" Pair.pp pair
  | Some `unsubscribe, Some id, _, _ ->
    Option.iter (Int32.Table.find rev_subs_depth id) ~f:begin fun symbol ->
      log_evt `unsubscribe symbol ;
      Pair.Table.remove subs_depth symbol
    end ;
    Int32.Table.remove rev_subs_depth id
  | Some `subscribe, Some id, Some (Ok pair), _ ->
    log_evt `subscribe pair ;
    begin
      match Int32.Table.find rev_subs_depth id with
      | Some p' when not (Pair.equal pair p') ->
        reject_market_depth_request addr w ~id
          "id %ld already used for pair %a" id Pair.pp pair
      | _ ->
        Pair.Table.add subs_depth pair id ;
        Int32.Table.set rev_subs_depth ~key:id ~data:pair ;
        write_market_depth_snapshot ~id addr w ~pair ~num_levels
    end
  | _ ->
    reject_market_depth_request addr w "invalid request"

let send_open_order_update w request_id nb_open_orders
    ~key:_ ~data:(symbol, { Rest.OpenOrder.id; side; price; qty; starting_qty; _ } ) i =
  let resp = DTC.default_order_update () in
  let status = if Float.equal qty starting_qty then
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
  resp.buy_sell <- Some (match side with Sell -> `sell | Buy -> `buy) ;
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
    let { Conn.orders ; _ } = Conn.find_exn addr in
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
  let { Conn.positions ; _ } = Conn.find_exn addr in
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
  resp.buy_sell <- Some (match side with Buy -> `buy | Sell -> `sell) ;
  resp.price <- Some price ;
  resp.quantity <- Some qty ;
  resp.date_time <- Some (int64_of_time ts) ;
  write_message w `historical_order_fill_response
    DTC.gen_historical_order_fill_response resp ;
  Int32.succ msg_number

let historical_order_fills addr w msg =
  let { Conn.trades ; _ } = Conn.find_exn addr in
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
  let c = Conn.find_exn addr in
  match req.trade_account with
  | None
  | Some "" ->
    Log.debug begin fun m ->
      m "<- [%a] AccountBalanceRequest (all accounts)" pp_print_addr c.addr
    end ;
    Conn.write_exchange_balance ?request_id:req.request_id ~msg_number:1 ~nb_msgs:2 c;
    Conn.write_margin_balance ?request_id:req.request_id ~msg_number:2 ~nb_msgs:2 c
  | Some account when String.equal account exchange_account ->
    Log.debug begin fun m ->
      m "<- [%a] AccountBalanceRequest (%s)" pp_print_addr c.addr account
    end ;
    Conn.write_exchange_balance ?request_id:req.request_id c
  | Some account when String.equal account margin_account ->
    Log.debug begin fun m ->
      m "<- [%a] AccountBalanceRequest (%s)" pp_print_addr c.addr account
    end ;
    Conn.write_margin_balance ?request_id:req.request_id c
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
  let side =
    match req.buy_sell with
    | Some `buy -> Fixtypes.Side.Buy
    | Some `sell -> Sell
    | _ -> invalid_arg "open_order_of_submit_new_single_order" in
  let price = Option.value ~default:0. req.price1 in
  let qty = Option.value_map req.quantity ~default:0. ~f:(( *. ) 1e-4) in
  let margin = if margin then 1 else 0 in
  Rest.OpenOrder.create ~id ~ts:(Time_ns.now ()) ~side
    ~price ~qty ~starting_qty:qty ~margin

(* req argument is normalized. *)
(* let submit_new_order
 *     ({ Conn.addr ; w ; key ; secret ; client_orders ; orders ; _ } as conn)
 *     (req : DTC.submit_new_single_order) =
 *   let symbol = Option.value_exn req.symbol in
 *   let side = Option.value_exn ~message:"submit_order: side" req.buy_sell in
 *   let price = Option.value_exn req.price1 in
 *   let qty = Option.value_map req.quantity ~default:0. ~f:(( *. ) 1e-4) in
 *   let margin = margin_enabled symbol in
 *   let tif = match req.time_in_force with
 *     | Some `tif_fill_or_kill -> Some `Fill_or_kill
 *     | Some `tif_immediate_or_cancel -> Some `Immediate_or_cancel
 *     | _ -> None
 *   in
 *   let order_f =
 *     if margin then Rest.submit_margin_order ?max_lending_rate:None
 *     else Rest.submit_order
 *   in
 *   Log.debug begin fun m ->
 *     m "-> [%a] Submit Order %s %f %f"
 *       pp_print_addr addr symbol price qty
 *   end ;
 *   order_f ~buf:buf_json ?tif ~key ~secret ~side ~symbol ~price ~qty () >>| function
 *   | Error Rest.Http_error.Poloniex msg ->
 *     reject_new_order w req "%s" msg
 *   | Error err ->
 *     Option.iter req.client_order_id ~f:begin fun id ->
 *       reject_new_order w req "%s: %s" id (Rest.Http_error.to_string err)
 *     end
 *   | Ok { id; trades; amount_unfilled } -> begin
 *       Int.Table.set client_orders ~key:id ~data:req ;
 *       Int.Table.set orders ~key:id
 *         ~data:(symbol, open_order_of_submit_new_single_order id req margin) ;
 *       Log.debug begin fun m ->
 *         m "<- [%a] Submit Order OK %d" pp_print_addr addr id
 *       end ;
 *       match trades, amount_unfilled with
 *       | [], _ ->
 *         send_new_order_update w req
 *           ~status:`order_status_open
 *           ~reason:`new_order_accepted
 *           ~server_order_id:id
 *           ~filled_qty:0.
 *           ~remaining_qty:qty
 *       | _trades, 0. ->
 *         send_new_order_update w req
 *           ~status:`order_status_filled
 *           ~reason:`order_filled
 *           ~server_order_id:id
 *           ~filled_qty:qty
 *           ~remaining_qty:0. ;
 *         if margin then
 *           Restsync.Default.push_nowait
 *             (fun () -> Conn.update_positions conn)
 *       | trades, _unfilled ->
 *         let trades = Rest.OrderResponse.trades_of_symbol trades symbol in
 *         let filled_qty =
 *           List.fold_left trades ~init:0. ~f:(fun a { qty ; _ } -> a +. qty) in
 *         let remaining_qty = qty -. filled_qty in
 *         send_new_order_update w req
 *           ~status:`order_status_partially_filled
 *           ~reason:`order_filled_partially
 *           ~server_order_id:id
 *           ~filled_qty
 *           ~remaining_qty ;
 *         if margin then
 *           Restsync.Default.push_nowait
 *             (fun () -> Conn.update_positions conn)
 *     end *)

(* let submit_new_single_order
 *     ({ Conn.w ; _ } as conn)
 *     (req : DTC.submit_new_single_order) =
 *   req.time_in_force <- begin
 *     match req.order_type with
 *     | Some `order_type_market -> Some `tif_fill_or_kill
 *     | _ -> req.time_in_force
 *   end ;
 *   begin match req.symbol, req.exchange with
 *     | Some symbol, Some exchange when
 *         String.Table.mem tickers symbol && exchange = my_exchange -> ()
 *     | _ ->
 *       reject_new_order w req "Unknown symbol or exchange" ;
 *       raise Exit
 *   end ;
 *   begin match Option.value ~default:`tif_unset req.time_in_force with
 *     | `tif_good_till_canceled
 *     | `tif_fill_or_kill
 *     | `tif_immediate_or_cancel -> ()
 *     | `tif_day ->
 *       req.time_in_force <- Some `tif_good_till_canceled
 *     | `tif_unset ->
 *       reject_new_order w req "Time in force unset" ;
 *       raise Exit
 *     | #DTC.time_in_force_enum ->
 *       reject_new_order w req "Unsupported time in force" ;
 *       raise Exit
 *   end ;
 *   begin match Option.value ~default:`order_type_unset req.order_type, req.price1 with
 *     | `order_type_market, _ ->
 *       req.price1 <-
 *         Option.(req.symbol >>= String.Table.find latest_trades >>| fun { price ; _ } -> price *. 2.)
 *     | `order_type_limit, Some price ->
 *       req.price1 <- Some price
 *     | `order_type_limit, None ->
 *       reject_new_order w req "Limit order without a price" ;
 *       raise Exit
 *     | #DTC.order_type_enum, _ ->
 *       reject_new_order w req "Unsupported order type" ;
 *       raise Exit
 *   end ;
 *   Restsync.Default.push_nowait (fun () -> submit_new_order conn req) *)

(* let submit_new_single_order addr msg =
 *   let conn = Conn.find_exn addr in
 *   let req = DTC.parse_submit_new_single_order msg in
 *   Log.debug begin fun m ->
 *     m "<- [%a] Submit New Single Order" pp_print_addr conn.addr
 *   end ;
 *   try submit_new_single_order conn req with
 *   | Exit -> ()
 *   | exn -> Log.err (fun m -> m "%a" Exn.pp exn) *)

(* let reject_cancel_order w (req : DTC.cancel_order) k =
 *   let update = DTC.default_order_update () in
 *   update.message_number <- Some 1l ;
 *   update.total_num_messages <- Some 1l ;
 *   update.client_order_id <- req.client_order_id ;
 *   update.server_order_id <- req.server_order_id ;
 *   update.order_status <- Some `order_status_open ;
 *   update.order_update_reason <- Some `order_cancel_rejected ;
 *   Printf.ksprintf begin fun info_text ->
 *     update.info_text <- Some info_text ;
 *     write_message w `order_update DTC.gen_order_update update
 *   end k
 * 
 * let send_cancel_update w server_order_id (req : DTC.Submit_new_single_order.t) =
 *   let update = DTC.default_order_update () in
 *   update.message_number <- Some 1l ;
 *   update.total_num_messages <- Some 1l ;
 *   update.symbol <- req.symbol ;
 *   update.exchange <- req.exchange ;
 *   update.order_type <- req.order_type ;
 *   update.buy_sell <- req.buy_sell ;
 *   update.order_quantity <- req.quantity ;
 *   update.price1 <- req.price1 ;
 *   update.price2 <- req.price2 ;
 *   update.order_status <- Some `order_status_canceled ;
 *   update.order_update_reason <- Some `order_canceled ;
 *   update.client_order_id <- req.client_order_id ;
 *   update.server_order_id <- Some server_order_id ;
 *   write_message w `order_update DTC.gen_order_update update
 * 
 * let submit_new_single_order_of_open_order symbol (order : Rest.OpenOrder.t) =
 *   let req = DTC.default_submit_new_single_order () in
 *   req.symbol <- Some symbol ;
 *   req.exchange <- Some my_exchange ;
 *   req.buy_sell <- Some order.side ;
 *   req.price1 <- Some order.price ;
 *   req.quantity <- Some order.starting_qty ;
 *   req
 * 
 * let cancel_order addr msg =
 *   let { Conn.w ; key ; secret ; client_orders ; orders ; _} =
 *     Conn.find_exn addr in
 *   let req = DTC.parse_cancel_order msg in
 *   match Option.map req.server_order_id ~f:Int.of_string with
 *   | None ->
 *     reject_cancel_order w req "Server order id not set"
 *   | Some order_id ->
 *     Log.debug begin fun m ->
 *       m "<- [%a] Order Cancel %d" pp_print_addr addr order_id
 *     end ;
 *     Restsync.Default.push_nowait begin fun () ->
 *       Rest.cancel_order ~key ~secret ~order_id () >>| function
 *       | Error Rest.Http_error.Poloniex msg ->
 *         reject_cancel_order w req "%s" msg
 *       | Error _ ->
 *         reject_cancel_order w req
 *           "exception raised while trying to cancel %d" order_id
 *       | Ok () ->
 *         Log.debug begin fun m ->
 *           m "-> [%a] Order Cancel OK %d" pp_print_addr addr order_id
 *         end ;
 *         let order_id_str = Int.to_string order_id in
 *         match Int.Table.find client_orders order_id,
 *               Int.Table.find orders order_id with
 *         | None, None ->
 *           Log.err begin fun m ->
 *             m "<- [%a] Unable to find order id %d in tables"
 *               pp_print_addr addr order_id
 *           end ;
 *           send_cancel_update w order_id_str
 *             (DTC.default_submit_new_single_order ())
 *         | Some client_order, _ ->
 *           Int.Table.remove orders order_id ;
 *           send_cancel_update w order_id_str client_order ;
 *         | None, Some (symbol, order) ->
 *           Log.err begin fun m ->
 *             m "[%a] Found open order %d but no matching client order"
 *               pp_print_addr addr order_id
 *           end ;
 *           send_cancel_update w order_id_str
 *             (submit_new_single_order_of_open_order symbol order)
 *     end *)

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

(* let cancel_replace_order addr msg =
 *   let { Conn.addr ; w ; key ; secret ; client_orders ; orders ; _ }
 *     = Conn.find_exn addr in
 *   let req = DTC.parse_cancel_replace_order msg in
 *   Log.debug begin fun m ->
 *     m "<- [%a] Cancel Replace Order" pp_print_addr addr
 *   end ;
 *   let tif = Option.value ~default:`tif_unset req.time_in_force in
 *   if tif <> `tif_unset then
 *     reject_cancel_replace_order addr w req
 *       "Modification of time in force is not supported by Poloniex"
 *   else
 *     match Option.map req.server_order_id ~f:Int.of_string, req.price1 with
 *     | None, _ ->
 *       reject_cancel_replace_order addr w req "Server order id is not set"
 *     | _, None ->
 *       reject_cancel_replace_order addr w req
 *         "Order modify without setting a price is not supported by Poloniex"
 *     | Some orig_server_id, Some price ->
 *       let qty = Option.map req.quantity ~f:(( *. ) 1e-4) in
 *       Restsync.Default.push_nowait begin fun () ->
 *         Rest.modify_order ~key ~secret ?qty ~price ~order_id:orig_server_id () >>| function
 *         | Error Rest.Http_error.Poloniex msg ->
 *           reject_cancel_replace_order addr w req
 *             "cancel order %d failed: %s" orig_server_id msg
 *         | Error _ ->
 *           reject_cancel_replace_order addr w req
 *             "cancel order %d failed" orig_server_id
 *         | Ok { id; trades = _ ; amount_unfilled } ->
 *           Log.debug begin fun m ->
 *             m "<- [%a] Cancel Replace Order %d -> %d OK"
 *               pp_print_addr addr orig_server_id id
 *           end ;
 *           let order_id_str = Int.to_string id in
 *           let amount_unfilled = amount_unfilled *. 1e4 in
 *           match Int.Table.find client_orders orig_server_id,
 *                 Int.Table.find orders orig_server_id with
 *           | None, None ->
 *             Log.err begin fun m ->
 *               m "[%a] Unable to find order id %d in tables"
 *                 pp_print_addr addr orig_server_id
 *             end ;
 *             send_cancel_replace_update w order_id_str amount_unfilled
 *               (DTC.default_submit_new_single_order ()) req
 *           | Some client_order, Some (symbol, open_order) ->
 *             Int.Table.remove client_orders orig_server_id ;
 *             Int.Table.remove orders orig_server_id ;
 *             Int.Table.set client_orders ~key:id ~data:client_order ;
 *             Int.Table.set orders ~key:id ~data:(symbol, { open_order with qty = amount_unfilled }) ;
 *             send_cancel_replace_update
 *               w order_id_str amount_unfilled client_order req
 *           | Some client_order, None ->
 *             Log.err begin fun m ->
 *               m "[%a] Found client order %d but no matching open order"
 *                 pp_print_addr addr orig_server_id
 *             end ;
 *             Int.Table.remove client_orders orig_server_id ;
 *             Int.Table.set client_orders ~key:id ~data:client_order ;
 *             send_cancel_replace_update
 *               w order_id_str amount_unfilled client_order req
 *           | None, Some (symbol, order) ->
 *             Log.err begin fun m ->
 *               m "[%a] Found open order %d but no matching client order"
 *                 pp_print_addr addr orig_server_id
 *             end ;
 *             send_cancel_replace_update w order_id_str amount_unfilled
 *               (submit_new_single_order_of_open_order symbol order) req
 *       end *)

let server_fun self addr r w =
  let on_connection_io_error exn =
    Conn.remove addr "connection_io_error %a" Exn.pp exn ;
    record_event self (E.nb_connected addr (Conn.length ())) ;
    log_event_now self (E.create addr (Connection_io_error exn)) ;
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
            record_event self (E.nb_connected addr (Conn.length ())) ;
            log_event_now self (E.create addr Logoff) ;
          | `encoding_request ->
            log_event_now self (E.create addr Encoding_request) ;
            begin match (Encoding.read (Bigstring.To_string.subo buf ~pos ~len:16)) with
              | None -> Log.err begin fun m ->
                  m "Invalid encoding request received"
                end
              | Some _ -> encoding_request addr w
            end
          | `logon_request ->
            let req = DTC.parse_logon_request msg in
            don't_wait_for begin
              log_event self (E.create addr (Logon_request req)) >>| fun () ->
              logon_request self addr w req
            end
          | `heartbeat ->
            log_event_now self (E.create addr Heartbeat_recv) ;
            Conn.record_hb addr
          | `security_definition_for_symbol_request ->
            let log_evt sym = log_event_now self
                (E.create addr (SecurityDefinitionForSymbolRequest sym)) in
            security_definition_request log_evt addr w msg
          | `market_data_request ->
            let log_evt action pair = log_event_now self
                (E.create addr (MarketDataRequest { action ; pair })) in
            market_data_request log_evt addr w msg
          | `market_depth_request ->
            let log_evt action pair = log_event_now self
                (E.create addr (MarketDepthRequest { action ; pair })) in
            market_depth_request log_evt addr w msg
          | `open_orders_request -> open_orders_request addr w msg
          | `current_positions_request -> current_positions_request addr w msg
          | `historical_order_fills_request -> historical_order_fills addr w msg
          | `trade_accounts_request -> trade_account_request addr w msg
          | `account_balance_request -> account_balance_request addr msg
          (* | `submit_new_single_order -> submit_new_single_order addr msg
           * | `cancel_order -> cancel_order addr msg
           * | `cancel_replace_order -> cancel_replace_order addr msg *)
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

  let on_launch_complete _ = Deferred.unit

  let on_request _self { R.ret } =
    return ret

  let on_close _self =
    Deferred.unit

  let on_launch self _name { V.server ; port } =
    let on_handler_error self addr exn =
      Conn.remove addr "handler_error %a" Exn.pp exn ;
      record_event self (E.nb_connected addr (Conn.length ())) ;
      log_event_now self (E.create addr (TCP_handler_error exn)) in
    let _stop_gc = Conn.gc () in
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
