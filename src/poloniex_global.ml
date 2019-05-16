open Core

open Plnx
module Rest = Plnx_rest
module DTC = Dtc_pb.Dtcprotocol_piqi

let src = Logs.Src.create "poloniex" ~doc:"Poloniex DTC"

module Log = (val Logs.src_log src : Logs.LOG)
module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

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

let buf_json = Bi_outbuf.create 4096

let depth_update = DTC.default_market_depth_update_level ()
let bidask_update = DTC.default_market_data_update_bid_ask ()
let trade_update = DTC.default_market_data_update_trade ()
let session_low_update = DTC.default_market_data_update_session_low ()
let session_high_update = DTC.default_market_data_update_session_high ()
