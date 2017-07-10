(* Continuously pump data from Poloniex and serves it with DTC *)

open Core
open Async
open Log.Global
open Cohttp_async

open Bs_devkit
open Plnx
module REST = Plnx_rest
module DTC = Dtc_pb.Dtcprotocol_piqi

module DB = struct
  include Tick.MakeLDB(LevelDB)
  let store_trade_in_db ?sync db ~ts ~price ~qty ~side =
    put_tick ?sync db @@ Tick.create ~ts ~side ~p:price ~v:qty ()
end

let buf = Bi_outbuf.create 4096

let datadir = ref (Filename.concat "data" "poloniex")
let dry_run = ref false

let db_path symbol =
  Filename.concat !datadir symbol

let dbs = String.Table.create ()

let mk_store_trade_in_db () =
  let tss = String.Table.create () in
  fun symbol { Trade.ts; price; qty; side } ->
    if !dry_run || side = `buy_sell_unset then ()
    else
      let db = String.Table.find_or_add dbs symbol
          ~default:(fun () -> DB.open_db @@ db_path symbol) in
      let ts = match String.Table.find tss symbol with
        | None ->
          String.Table.add_exn tss symbol (ts, 0); ts
        | Some (old_ts, _) when old_ts <> ts ->
          String.Table.set tss symbol (ts, 0); ts
        | Some (_, n) ->
          String.Table.set tss symbol (ts, succ n);
          Time_ns.(add ts @@ Span.of_int_ns @@ succ n)
      in
      let price = satoshis_int_of_float_exn price |> Int63.of_int in
      let qty = satoshis_int_of_float_exn qty |> Int63.of_int in
      DB.store_trade_in_db db ~ts ~price ~qty ~side

let store_trade_in_db = mk_store_trade_in_db ()

let write_message w (typ : DTC.dtcmessage_type) gen msg =
  let typ =
    Piqirun.(DTC.gen_dtcmessage_type typ |> to_string |> init_from_string |> int_of_varint) in
  let msg = (gen msg |> Piqirun.to_string) in
  let header = Bytes.create 4 in
  Binary_packing.pack_unsigned_16_little_endian ~buf:header ~pos:0 (4 + String.length msg) ;
  Binary_packing.pack_unsigned_16_little_endian ~buf:header ~pos:2 typ ;
  Writer.write w header ;
  Writer.write w msg

module Granulator = struct
  type t = {
    nb_streamed : int ;
    nb_processed : int ;
    start_ts : Time_ns.t ;
    end_ts : Time_ns.t ;
    record : DTC.Historical_price_data_record_response.t ;
  }

  let create
      ?(nb_streamed=0) ?(nb_processed=0) ?request_id
      ~ts ~price ~qty ~side ~span () =
    let record = DTC.default_historical_price_data_record_response () in
    record.request_id <- request_id ;
    record.start_date_time <- Some (seconds_int64_of_ts ts) ;
    record.open_price <- Some price ;
    record.high_price <- Some price ;
    record.low_price <- Some price ;
    record.last_price <- Some price ;
    record.volume <- Some qty ;
    record.num_trades <- Some 1l ;
    record.bid_volume <- if side = `buy then Some qty else None ;
    record.ask_volume <- if side = `sell then Some qty else None ;
    {
      nb_streamed ;
      nb_processed ;
      start_ts = ts ;
      end_ts = Time_ns.(add ts @@ Span.(span - nanosecond)) ;
      record ;
    }

  let add_tick ?request_id ~w ~span ~ts ~price ~qty ~side = function
    | None ->
      create ?request_id ~span ~ts ~price ~qty ~side ()
    | Some r ->
      if Time_ns.between ts ~low:r.start_ts ~high:r.end_ts then begin
        r.record.high_price <-
          Option.map r.record.high_price ~f:(Float.max price) ;
        r.record.low_price <-
          Option.map r.record.low_price ~f:(Float.min price) ;
        r.record.last_price <- Some price ;
        r.record.volume <-
          Option.map r.record.volume ~f:Float.(( + ) qty) ;
        r.record.num_trades <-
          Option.map r.record.num_trades ~f:Int32.succ ;
        r.record.bid_volume <-
          Option.map r.record.bid_volume ~f:begin fun b ->
            if side = `buy then b +. qty else b
          end ;
        r.record.ask_volume <-
          Option.map r.record.ask_volume ~f:begin fun a ->
            if side = `sell then a +. qty else a
          end ;
        { r with nb_processed = succ r.nb_processed }
      end
      else begin
        write_message w `historical_price_data_record_response
          DTC.gen_historical_price_data_record_response r.record ;
        create
          ?request_id
          ~nb_streamed:(succ r.nb_streamed)
          ~nb_processed:r.nb_processed
          ~span ~ts ~price ~qty ~side ()
      end
end

let rec pump start_ts symbol =
  let trades = REST.all_trades ~start_ts ~log:(Lazy.force log) symbol in
  Pipe.fold_without_pushback trades
    ~init:(0, Time_ns.max_value) ~f:begin fun (nb_trades, last_ts) t ->
    store_trade_in_db symbol t ;
    succ nb_trades, t.ts
  end >>| fun (nb_trades, last_ts) ->
  let last_ts_str = Time_ns.to_string last_ts in
  debug "pumped %d trades up to to %s" nb_trades last_ts_str ;
  if not !dry_run then
    Out_channel.write_all (db_path ("start_" ^ symbol)) ~data:last_ts_str

(* A DTC Historical Price Server. *)

let encoding_request addr w req =
  debug "<- [%s] Encoding Request" addr ;
  Dtc_pb.Encoding.(to_string (Response { version = 7 ; encoding = Protobuf })) |>
  Writer.write w ;
  debug "-> [%s] Encoding Response" addr

let accept_logon_request addr w req =
  let r = DTC.default_logon_response () in
  r.protocol_version <- Some 7l ;
  r.server_name <- Some "Poloniex Data" ;
  r.result <- Some `logon_success ;
  r.result_text <- Some "OK" ;
  r.symbol_exchange_delimiter <- Some "-" ;
  r.historical_price_data_supported <- Some true ;
  r.one_historical_price_data_request_per_connection <- Some true ;
  write_message w `logon_response DTC.gen_logon_response r ;
  debug "-> [%s] Logon Response" addr

let logon_request addr w msg =
  let req = DTC.parse_logon_request msg in
  debug "<- [%s] Logon Request" addr ;
  accept_logon_request addr w req

let heartbeat addr w msg =
  debug "<- [%s] Heartbeat" addr

let reject_historical_price_data_request ?reason_code w (req : DTC.Historical_price_data_request.t) k =
  let rej = DTC.default_historical_price_data_reject () in
  rej.request_id <- req.request_id ;
  rej.reject_reason_code <- reason_code ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `historical_price_data_reject
      DTC.gen_historical_price_data_reject rej ;
    debug "-> HistoricalPriceData reject %s" reject_text
  end k

let span_of_interval = function
  | `interval_10_seconds -> Time_ns.Span.of_int_sec 10
  | `interval_1_day -> Time_ns.Span.of_day 1.
  | `interval_1_minute -> Time_ns.Span.of_int_sec 60
  | `interval_1_second -> Time_ns.Span.of_int_sec 1
  | `interval_1_week -> Time_ns.Span.of_day 7.
  | `interval_2_seconds -> Time_ns.Span.of_int_sec 2
  | `interval_30_seconds -> Time_ns.Span.of_int_sec 30
  | `interval_4_seconds -> Time_ns.Span.of_int_sec 4
  | `interval_5_seconds -> Time_ns.Span.of_int_sec 5
  | `interval_tick -> Time_ns.Span.zero

let max_int_value = Int64.of_int_exn Int.max_value
let start_key = Bytes.create 8

let stream_tick_responses symbol
    ?stop db w (req : DTC.Historical_price_data_request.t) start =
  info "Streaming %s from %s (tick)" symbol Time_ns.(to_string start) ;
  let resp = DTC.default_historical_price_data_tick_record_response () in
  resp.request_id <- req.request_id ;
  resp.is_final_record <- Some false ;
  let nb_streamed =
    DB.HL.fold_left db ?stop ~start ~init:0 ~f:begin fun a t ->
      let p = Int63.to_float t.Tick.p /. 1e8 in
      let v = Int63.to_float t.v /. 1e8 in
      let side = match t.side with
        | `buy -> `at_bid
        | `sell -> `at_ask
        | `buy_sell_unset -> `bid_ask_unset in
      resp.date_time <- Some (float_of_ts t.ts) ;
      resp.price <- Some p ;
      resp.volume <- Some v ;
      resp.at_bid_or_ask <- Some side ;
      write_message w `historical_price_data_tick_record_response
        DTC.gen_historical_price_data_tick_record_response resp ;
      succ a ;
    end in
  let resp = DTC.default_historical_price_data_tick_record_response () in
  resp.request_id <- req.request_id ;
  resp.is_final_record <- Some true ;
  write_message w `historical_price_data_tick_record_response
    DTC.gen_historical_price_data_tick_record_response resp ;
  nb_streamed, nb_streamed

let stream_record_responses symbol
    ?stop db w (req : DTC.Historical_price_data_request.t) start span =
  info "Streaming %s from %s (%s)" symbol
    Time_ns.(to_string start) (Time_ns.Span.to_string span) ;
  let add_tick = Granulator.add_tick ~w ?request_id:req.request_id ~span in
  let r =
    DB.HL.fold_left db ~start ?stop ~init:None ~f:begin fun a t ->
      let price = Int63.to_float t.p /. 1e8 in
      let qty = Int63.to_float t.v /. 1e8 in
      Some (add_tick ~ts:t.ts ~price ~qty ~side:t.side a)
    end in
  Option.iter r ~f:begin fun r ->
    write_message w `historical_price_data_record_response
      DTC.gen_historical_price_data_record_response r.record ;
  end ;
  let resp = DTC.default_historical_price_data_record_response () in
  resp.request_id <- req.request_id ;
  resp.is_final_record <- Some true ;
  write_message w `historical_price_data_record_response
    DTC.gen_historical_price_data_record_response resp ;
  Option.value_map r ~default:0 ~f:(fun r -> r.nb_streamed),
  Option.value_map r ~default:0 ~f:(fun r -> r.nb_processed)

let accept_historical_price_data_request
    w (req : DTC.Historical_price_data_request.t) db symbol span =
  let hdr = DTC.default_historical_price_data_response_header () in
  hdr.request_id <- req.request_id ;
  hdr.record_interval <- req.record_interval ;
  hdr.use_zlib_compression <- Some false ;
  hdr.int_to_float_price_divisor <- Some 1e8 ;
  write_message w `historical_price_data_response_header
    DTC.gen_historical_price_data_response_header hdr ;
  let start =
    Option.value_map req.start_date_time ~default:0L ~f:Int64.(( * ) 1_000_000_000L) |>
    Int64.to_int_exn |>
    Time_ns.of_int_ns_since_epoch |>
    Time_ns.(max epoch) in
  let stop =
    Option.value_map req.end_date_time ~default:0L ~f:Int64.(( * ) 1_000_000_000L) |>
    Int64.to_int_exn |>
    Time_ns.of_int_ns_since_epoch in
  let stop = if stop = Time_ns.epoch then None else Some stop in
  if span = Time_ns.Span.zero then (* streaming tick responses *)
    stream_tick_responses symbol ?stop db w req start
  else
    stream_record_responses symbol ?stop db w req start span

let historical_price_data_request addr w msg =
  let req = DTC.parse_historical_price_data_request msg in
  begin match req.symbol, req.exchange with
    | Some symbol, Some exchange ->
      debug "<- [%s] Historical Data Request %s %s" addr symbol exchange ;
    | _ -> ()
  end ;
  let span =
    Option.value_map ~default:Time_ns.Span.zero
      ~f:span_of_interval req.record_interval in
  match req.symbol, req.exchange with
  | None, _ ->
    reject_historical_price_data_request
      ~reason_code:`hpdr_unable_to_serve_data_do_not_retry
      w req "Symbol not specified" ;
    raise Exit
  | Some symbol, _ ->
    match String.Table.find dbs symbol with
    | None ->
      reject_historical_price_data_request
        ~reason_code:`hpdr_unable_to_serve_data_do_not_retry
        w req "No such symbol" ;
      raise Exit
    | Some db -> don't_wait_for begin
        In_thread.run begin fun () ->
          accept_historical_price_data_request w req db symbol span
        end >>| fun (nb_streamed, nb_processed) ->
        info "Streamed %d/%d records from %s"
          nb_streamed nb_processed symbol
      end

let dtcserver ~server ~port =
  let server_fun addr r w =
    let addr = Socket.Address.Inet.to_string addr in
    (* So that process does not allocate all the time. *)
    let rec handle_chunk consumed buf ~pos ~len =
      if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        debug "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen;
        if len < msglen then return @@ `Consumed (consumed, `Need msglen)
        else begin
          let msgtype_int = Bigstring.unsafe_get_int16_le buf ~pos:(pos+2) in
          let msgtype : DTC.dtcmessage_type =
            DTC.parse_dtcmessage_type (Piqirun.Varint msgtype_int) in
          let msg_str = Bigstring.To_string.subo buf ~pos:(pos+4) ~len:(msglen-4) in
          let msg = Piqirun.init_from_string msg_str in
          begin match msgtype with
            | `encoding_request ->
              begin match (Dtc_pb.Encoding.read (Bigstring.To_string.subo buf ~pos ~len:16)) with
                | None -> error "Invalid encoding request received"
                | Some msg -> encoding_request addr w msg
              end
            | `logon_request -> logon_request addr w msg
            | `heartbeat -> heartbeat addr w msg
            | `historical_price_data_request -> historical_price_data_request addr w msg
            | #DTC.dtcmessage_type ->
              error "Unknown msg type %d" msgtype_int
          end ;
          handle_chunk (consumed + msglen) buf (pos + msglen) (len - msglen)
        end
    in
    let on_connection_io_error exn =
      error "on_connection_io_error (%s): %s" addr Exn.(to_string exn)
    in
    let cleanup () =
      info "client %s disconnected" addr ;
      Deferred.all_unit [Writer.close w; Reader.close r]
    in
    Deferred.ignore @@ Monitor.protect ~finally:cleanup begin fun () ->
      Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_connection_io_error;
      Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk 0))
    end
  in
  let on_handler_error_f addr = function
    | Exit -> ()
    | exn ->
      error "on_handler_error (%s): %s"
        Socket.Address.(to_string addr) Exn.(to_string exn)
  in
  Conduit_async.serve
    ~on_handler_error:(`Call on_handler_error_f)
    server (Tcp.on_port port) server_fun

let load_instruments () =
  REST.symbols ~buf ~log:(Lazy.force log) () >>|
  Result.map ~f:begin fun symbols ->
    List.map symbols ~f:begin fun key ->
      String.Table.add_exn dbs ~key ~data:(DB.open_db @@ db_path key);
      info "Opened DB %s" key ;
      key
    end
  end

let start_ts_of_symbol ?start symbol =
  match start with
  | Some days -> Time_ns.(sub (now ()) (Span.of_day days))
  | None ->
    try Time_ns.of_string @@ In_channel.read_all @@ db_path ("start_" ^ symbol)
    with exn ->
      error "Unable to read start file, using epoch %s" (Exn.to_string exn) ;
      Time_ns.epoch

let run ?start port no_pump symbols_in_use =
  load_instruments () >>= function
  | Error err ->
    error "%s" (REST.Http_error.to_string err) ;
    Deferred.unit
  | Ok all_symbols ->
    let symbols_in_use =
      String.Set.(inter (of_list all_symbols) (of_list symbols_in_use) |> to_list) in
    info "Data server starting";
    dtcserver ~server:`TCP ~port >>= fun server ->
    Deferred.all_unit [
      Tcp.Server.close_finished server ;
      if no_pump then Deferred.unit
      else
        Deferred.List.iter ~how:`Sequential symbols_in_use ~f:begin fun symbol ->
          let start_ts = start_ts_of_symbol ?start symbol in
          pump start_ts symbol
        end
    ]

let main dry_run' no_pump start port daemon datadir' pidfile logfile loglevel symbols () =
  dry_run := dry_run';
  datadir := datadir';
  set_level @@ loglevel_of_int loglevel;
  if daemon then Daemon.daemonize ~cd:"." ();
  Signal.handle Signal.terminating ~f:begin fun _ ->
    info "Data server stopping";
    String.Table.iter dbs ~f:DB.close;
    info "Saved %d dbs" @@ String.Table.length dbs;
    don't_wait_for @@ Shutdown.exit 0
  end ;
  stage begin fun `Scheduler_started ->
    Lock_file.create_exn pidfile >>= fun () ->
    Writer.open_file ~append:true logfile >>= fun log_writer ->
    set_output Log.Output.[stderr (); writer `Text log_writer];
    run ?start port no_pump symbols
  end

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-dry-run" no_arg ~doc:" Do not write trades in DBs"
    +> flag "-no-pump" no_arg ~doc:" Do not pump trades"
    +> flag "-start" (optional float) ~doc:"float Start gathering history N days in the past (default: use start file)"
    +> flag "-port" (optional_with_default 5574 int) ~doc:"int TCP port to use (5574)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-datadir" (optional_with_default (Filename.concat "data" "poloniex") string) ~doc:"path Where to store DBs (data)"
    +> flag "-pidfile" (optional_with_default (Filename.concat "run" "plnx_data.pid") string) ~doc:"filename Path of the pid file (run/plnx_data.pid)"
    +> flag "-logfile" (optional_with_default (Filename.concat "log" "plnx_data.log") string) ~doc:"filename Path of the log file (log/plnx_data.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
    +> anon (sequence ("symbol" %: string))
  in
  Command.Staged.async ~summary:"Poloniex data aggregator" spec main

let () = Command.run command
