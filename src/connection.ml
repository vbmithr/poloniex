open Core
open Async

module Rest = Plnx_rest
module DTC = Dtc_pb.Dtcprotocol_piqi

open Bmex_common
open Poloniex_global
open Poloniex_util

type t = {
  addr: Socket.Address.Inet.t;
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

module AddrMap = Map.Make(Socket.Address.Inet.Blocking_sexp)

let active = ref AddrMap.empty

let find = AddrMap.find !active
let find_exn = AddrMap.find_exn !active
let set ~key ~data =
  active := AddrMap.set !active ~key ~data
let remove k =
  active := AddrMap.remove !active k

let iter = AddrMap.iter !active

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

let update_positions { addr; w; key; secret; positions ; _ } =
  Rest.margin_positions ~buf:buf_json ~key ~secret () >>= function
  | Error err ->
    Logs_async.err begin fun m ->
      m "update positions (%a): %a"
        pp_print_addr addr Rest.Http_error.pp err
    end
  | Ok ps -> List.iter ps ~f:begin fun (symbol, p) ->
      match p with
      | None ->
        String.Table.remove positions symbol ;
        write_position_update w symbol
      | Some ({ price; qty; _ } as p) ->
        String.Table.set positions ~key:symbol ~data:p ;
        write_position_update w symbol ~price ~qty:(qty *. 1e4)
    end ;
    Deferred.unit

let update_orders { addr ; key; secret; orders ; _ } =
  Rest.open_orders ~buf:buf_json ~key ~secret () >>= function
  | Error err ->
    Logs_async.err begin fun m ->
      m "update orders (%a): %a" pp_print_addr addr Rest.Http_error.pp err
    end
  | Ok os ->
    Int.Table.clear orders;
    List.iter os ~f:begin fun (symbol, os) ->
      List.iter os ~f:begin fun o ->
        Logs.debug begin fun m ->
          m "<- [%a] Add %d in order table" pp_print_addr addr o.id
        end ;
        Int.Table.set orders ~key:o.id ~data:(symbol, o)
      end
    end ;
    Deferred.unit

let update_trades { addr; key; secret; trades ; _ } =
  Rest.trade_history ~buf:buf_json ~key ~secret () >>= function
  | Error err ->
    Logs_async.err begin fun m ->
      m "update trades (%a): %a" pp_print_addr addr Rest.Http_error.pp err
    end
  | Ok ts ->
    List.iter ts ~f:begin fun (symbol, ts) ->
      let old_ts =
        String.Table.find trades symbol |>
        Option.value ~default:Rest.TradeHistory.Set.empty in
      let cur_ts = Rest.TradeHistory.Set.of_list ts in
      let new_ts = Rest.TradeHistory.Set.diff cur_ts old_ts in
      String.Table.set trades ~key:symbol ~data:cur_ts;
      Rest.TradeHistory.Set.iter new_ts ~f:ignore (* TODO: send order update messages *)
    end ;
    Deferred.unit

let write_margin_balance
    ?request_id
    ?(nb_msgs=1)
    ?(msg_number=1) { addr; w; margin ; _ } =
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
  Logs.debug begin fun m ->
    m "-> %a AccountBalanceUpdate %s (%d/%d)"
      pp_print_addr addr margin_account msg_number nb_msgs
  end

let write_exchange_balance
    ?request_id
    ?(nb_msgs=1)
    ?(msg_number=1) { addr; w; b_exchange ; _ } =
  let b = String.Table.find b_exchange "BTC" |>
          Option.map ~f:begin fun { Rest.Balance.available; on_orders ; _ } ->
            available *. 1e3, (available -. on_orders) *. 1e3
          end
  in
  let securities_value =
    String.Table.fold b_exchange ~init:0.
      ~f:begin fun ~key:_ ~data:{ Rest.Balance.btc_value ; _ } a ->
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
  Logs.debug begin fun m ->
    m "-> %a AccountBalanceUpdate %s (%d/%d)"
      pp_print_addr addr exchange_account msg_number nb_msgs
  end

let update_margin ({ key ; secret ; _ } as conn) =
  Rest.margin_account_summary ~buf:buf_json ~key ~secret () >>= function
  | Error err ->
    Logs_async.err (fun m -> m "%a" Rest.Http_error.pp err)
  | Ok m ->
    conn.margin <- m ;
    Deferred.unit

let update_positive_balances ({ key ; secret ; b_margin ; _ } as conn) =
  Rest.positive_balances ~buf:buf_json ~key ~secret () >>= function
  | Error err ->
    Logs_async.err begin fun m ->
      m "%a" Rest.Http_error.pp err
    end
  | Ok bs ->
    String.Table.clear b_margin;
    List.Assoc.find ~equal:(=) bs Margin |>
    Option.iter ~f:begin
      List.iter ~f:(fun (c, b) -> String.Table.add_exn b_margin ~key:c ~data:b)
    end ;
    write_margin_balance conn ;
    Deferred.unit

let update_balances ({ key ; secret ; b_exchange ; _ } as conn) =
  Rest.balances ~buf:buf_json ~all:false ~key ~secret () >>= function
  | Error err ->
    Logs_async.err begin fun m ->
      m "%a" Rest.Http_error.pp err
    end
  | Ok bs ->
    String.Table.clear b_exchange;
    List.iter bs ~f:(fun (c, b) -> String.Table.add_exn b_exchange ~key:c ~data:b) ;
    write_exchange_balance conn ;
    Deferred.unit

let update_connection conn span =
  Clock_ns.every
    ~stop:(Writer.close_started conn.w)
    ~continue_on_error:true
    span
    begin fun () ->
      let open Restsync.Default in
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
