(* Continuously pump data from Poloniex and serves it with DTC *)

open Core
open Async

open Bs_devkit
open Plnx
open Bmex_common

module REST = Plnx_rest
module DTC = Dtc_pb.Dtcprotocol_piqi

module DB = struct
  include Tick.MakeLDB(LevelDB)
  let store_trade_in_db ?sync db ~ts ~price ~qty ~side =
    put_tick ?sync db @@ Tick.create ~ts ~side ~p:price ~v:qty ()
end

let src =
  Logs.Src.create "poloniex.data" ~doc:"Poloniex data collector"

module CtrlFile = struct
  type t = {
    fn : string ;
    bitv : Bitv.t
  }

  let open_file fn =
    try
      let ic = Caml.open_in_bin fn in
      try
        let bitv = Bitv.input_bin ic in
        Caml.close_in ic ;
        { fn ; bitv }
      with exn ->
        Caml.close_in ic ;
        raise exn
    with exn ->
      Logs.err ~src (fun m -> m "CtrlFile.open_file: %a" Exn.pp exn) ;
      { fn ; bitv = Bitv.create (365 * 24 * 10) false }

  let close { fn ; bitv } =
    let oc = Caml.open_out_bin fn in
    try
      Bitv.output_bin oc bitv ;
      Caml.close_out oc
    with exn ->
      Caml.close_out oc ;
      raise exn

  let genesis_date =
    Date.create_exn ~y:2017 ~m:Month.Jan ~d:1

  let genesis_ts =
    Time_ns.(of_date_ofday ~zone:Time.Zone.utc genesis_date Ofday.start_of_day)

  let jobs ?(start=genesis_date) { bitv ; _ } f =
    let idx = 24 * Date.diff start genesis_date in
    let now_ts = Time_ns.now () in
    let rec inner (i, thunks) =
      let start_ts = Time_ns.(add genesis_ts (Span.of_int_sec (i * 3600))) in
      let end_ts = Time_ns.(add start_ts (Span.of_int_sec 3600)) in
      let latest = Time_ns.(end_ts >= now_ts) in
      let thunks =
        if i >= 0 then begin
          if not (Bitv.get bitv i) || latest then
            begin fun () -> f ~start_ts ~end_ts >>| function
              | Error err -> Logs.err ~src (fun m -> m "%a" REST.Http_error.pp err)
              | Ok _ -> Bitv.set bitv i true
            end :: thunks
          else thunks
        end
        else begin fun () ->
          f ~start_ts ~end_ts >>| Result.iter_error ~f:begin fun err ->
            Logs.err ~src (fun m -> m "%a" REST.Http_error.pp err)
          end
        end :: thunks
      in
      if latest then thunks
      else inner (succ i, thunks)
    in inner (idx, [])
end

module Instrument = struct
  type t = {
    db : DB.db ;
    ctrl : CtrlFile.t ;
  }

  let create ~db ~ctrl = { db ; ctrl }

  let ( // ) = Filename.concat

  let datadir = ref ("data" // "poloniex")
  let set_datadir d = datadir := d

  let db_path symbol =
    !datadir // "db" // symbol

  let ctrl_path symbol =
    !datadir // "ctrl" // symbol

  let load symbols =
    let open Deferred.Result.Monad_infix in
    REST.symbols () >>| fun all_symbols ->
    let symbols_in_use =
      if String.Set.is_empty symbols then all_symbols
      else String.Set.(inter (of_list all_symbols) symbols |> to_list) in
    List.map symbols_in_use ~f:begin fun symbol ->
      let db = DB.open_db (db_path symbol) in
      let ctrl = CtrlFile.open_file (ctrl_path symbol) in
      Logs.info ~src (fun m -> m "Loaded instrument %s" symbol) ;
      symbol, create ~db ~ctrl
    end

  let active : t String.Table.t = String.Table.create ()
  let find = String.Table.find active
  let find_exn = String.Table.find_exn active

  let load symbols =
    Deferred.Result.map (load symbols) ~f:begin fun instruments ->
      List.fold_left instruments ~init:[] ~f:begin fun a (symbol, i) ->
        String.Table.add_exn active ~key:symbol ~data:i ;
        symbol :: a
      end
    end

  let thunks_exn ?start symbol f =
    let { ctrl ; _ } = find_exn symbol in
    CtrlFile.jobs ?start ctrl f

  let close { db ; ctrl } =
    DB.close db ;
    CtrlFile.close ctrl

  let shutdown () =
    let nb_closed =
      String.Table.fold active ~init:0 ~f:begin fun ~key:_ ~data a ->
        close data ;
        succ a
      end in
    Logs.info ~src (fun m -> m "Saved %d dbs" nb_closed)
end

let dry_run = ref false

let tss = String.Table.create ()

let store_trade_in_db symbol { Trade.ts; price; qty; side; _ } =
  if !dry_run || side = `buy_sell_unset then ()
  else
    let { Instrument.db; _ } = Instrument.find_exn symbol in
    let ts = match String.Table.find tss symbol with
      | None ->
        String.Table.add_exn tss ~key:symbol ~data:(ts, 0);
        REST.of_ptime ts
      | Some (old_ts, _) when old_ts <> ts ->
        String.Table.set tss ~key:symbol ~data:(ts, 0);
        REST.of_ptime ts
      | Some (_, n) ->
        String.Table.set tss ~key:symbol ~data:(ts, succ n);
        Time_ns.(add (REST.of_ptime ts) @@ Span.of_int_ns @@ succ n)
    in
    let price = satoshis_int_of_float_exn price |> Int63.of_int in
    let qty = satoshis_int_of_float_exn qty |> Int63.of_int in
    DB.store_trade_in_db db ~ts ~price ~qty ~side

let pump symbol ~start_ts ~end_ts =
  REST.trades ~start_ts ~end_ts symbol >>= function
  | Error err -> return (Error err)
  | Ok trades ->
    Pipe.fold_without_pushback trades
      ~init:(0, Time_ns.max_value) ~f:begin fun (nb_trades, _last_ts) t ->
      store_trade_in_db symbol t ;
      succ nb_trades, REST.of_ptime t.ts
    end >>= fun (nb_trades, _last_ts) ->
    Logs_async.debug ~src begin fun m ->
      m "pumped %d trades from %a to %a" nb_trades
        Time_ns.pp start_ts Time_ns.pp end_ts
    end >>= fun () ->
    Clock_ns.after (Time_ns.Span.of_int_ms 167) >>| fun () ->
    Ok ()

(* A DTC Historical Price Server. *)

let encoding_request addr w =
  Logs.debug ~src (fun m -> m "<- [%s] Encoding Request" addr) ;
  Dtc_pb.Encoding.(to_string (Response { version = 7 ; encoding = Protobuf })) |>
  Writer.write w ;
  Logs.debug ~src (fun m -> m "-> [%s] Encoding Response" addr)

let accept_logon_request addr w =
  let r = DTC.default_logon_response () in
  r.protocol_version <- Some 7l ;
  r.server_name <- Some "Poloniex Data" ;
  r.result <- Some `logon_success ;
  r.result_text <- Some "OK" ;
  r.symbol_exchange_delimiter <- Some "-" ;
  r.historical_price_data_supported <- Some true ;
  r.one_historical_price_data_request_per_connection <- Some true ;
  write_message w `logon_response DTC.gen_logon_response r ;
  Logs.debug ~src (fun m -> m "-> [%s] Logon Response" addr)

let logon_request addr w =
  Logs.debug ~src (fun m -> m "<- [%s] Logon Request" addr) ;
  accept_logon_request addr w

let heartbeat addr =
  Logs.debug ~src (fun m -> m "<- [%s] Heartbeat" addr)

let reject_historical_price_data_request ?reason_code w (req : DTC.Historical_price_data_request.t) k =
  let rej = DTC.default_historical_price_data_reject () in
  rej.request_id <- req.request_id ;
  rej.reject_reason_code <- reason_code ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `historical_price_data_reject
      DTC.gen_historical_price_data_reject rej ;
    Logs.debug ~src (fun m -> m "-> HistoricalPriceData reject %s" reject_text)
  end k

let max_int_value = Int64.of_int_exn Int.max_value
let start_key = Bytes.create 8

let stream_tick_responses symbol
    ?stop db w (req : DTC.Historical_price_data_request.t) start =
  Logs.info ~src (fun m -> m "Streaming %s from %a (tick)" symbol Time_ns.pp start) ;
  let resp = DTC.default_historical_price_data_tick_record_response () in
  resp.request_id <- req.request_id ;
  resp.is_final_record <- Some false ;
  let nb_streamed =
    DB.HL.fold_left db ?stop ~start ~init:0 ~f:begin fun a t ->
      let p = Int63.to_float t.Tick.p /. 1e8 in
      let v = Int63.to_float t.v /. 1e8 in
      let side = match t.side with
        | `buy -> `at_ask
        | `sell -> `at_bid
        | `buy_sell_unset -> `bid_ask_unset in
      resp.date_time <- Some (seconds_float_of_ts t.ts) ;
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

let accept_historical_price_data_request
    w (req : DTC.Historical_price_data_request.t) db symbol =
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
  stream_tick_responses symbol ?stop db w req start

let historical_price_data_request addr w msg =
  let req = DTC.parse_historical_price_data_request msg in
  begin match req.symbol, req.exchange with
    | Some symbol, Some exchange ->
      Logs.debug ~src (fun m -> m "<- [%s] Historical Data Request %s %s" addr symbol exchange) ;
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
    match Instrument.find symbol with
    | None ->
      reject_historical_price_data_request
        ~reason_code:`hpdr_unable_to_serve_data_do_not_retry
        w req "No such symbol" ;
      raise Exit
    | Some { db ; _ } ->
      if span > Time_ns.Span.zero then
        reject_historical_price_data_request
          ~reason_code:`hpdr_unable_to_serve_data_do_not_retry
          w req "Server can only stream ticks"
      else
        don't_wait_for begin
          In_thread.run begin fun () ->
            accept_historical_price_data_request w req db symbol
          end >>| fun (nb_streamed, nb_processed) ->
          Logs.info ~src begin fun m ->
            m "Streamed %d/%d records from %s"
              nb_streamed nb_processed symbol
          end
        end

let dtcserver ~server ~port =
  let server_fun addr r w =
    let addr = Socket.Address.Inet.to_string addr in
    (* So that process does not allocate all the time. *)
    let rec handle_chunk consumed buf ~pos ~len =
      if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        Logs.debug ~src begin fun m ->
          m "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen
        end ;
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
                | None -> Logs.err ~src (fun m -> m "Invalid encoding request received")
                | Some _ -> encoding_request addr w
              end
            | `logon_request -> logon_request addr w
            | `heartbeat -> heartbeat addr
            | `historical_price_data_request -> historical_price_data_request addr w msg
            | #DTC.dtcmessage_type ->
              Logs.err ~src (fun m -> m "Unknown msg type %d" msgtype_int)
          end ;
          handle_chunk (consumed + msglen) buf ~pos:(pos + msglen) ~len:(len - msglen)
        end
    in
    let on_connection_io_error exn =
      Logs.err ~src (fun m -> m "on_connection_io_error (%s): %a" addr Exn.pp exn)
    in
    let cleanup () =
      Logs_async.info ~src (fun m -> m "client %s disconnected" addr) >>= fun () ->
      Deferred.all_unit [Writer.close w; Reader.close r]
    in
    Deferred.ignore @@ Monitor.protect ~finally:cleanup begin fun () ->
      Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_connection_io_error;
      Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk 0))
    end
  in
  let on_handler_error_f addr exn =
    match Monitor.extract_exn exn with
    | Exit -> ()
    | exn ->
      Logs.err ~src begin fun m ->
        m "on_handler_error (%s): %a"
          Socket.Address.(to_string addr) Exn.pp exn
      end
  in
  Conduit_async.serve
    ~on_handler_error:(`Call on_handler_error_f)
    server (Tcp.Where_to_listen.of_port port) server_fun

let run ?start port no_pump symbols =
  Instrument.load symbols >>= function
  | Error err ->
    Logs_async.err ~src (fun m -> m "%a" REST.Http_error.pp err)
  | Ok symbols ->
    Logs_async.info ~src (fun m -> m "Data server starting") >>= fun () ->
    dtcserver ~server:`TCP ~port >>= fun server ->
    Deferred.all_unit [
      Tcp.Server.close_finished server ;
      if no_pump then Deferred.unit
      else
        let thunks = List.fold_left symbols ~init:[] ~f:begin fun a symbol ->
            List.rev_append (Instrument.thunks_exn ?start symbol (pump symbol)) a
          end in
        Deferred.List.iter thunks ~how:`Sequential ~f:(fun f -> f ())
    ]

let main dry_run' no_pump start port datadir symbols =
  dry_run := dry_run';
  Instrument.set_datadir datadir ;
  Signal.handle Signal.terminating ~f:begin fun _ ->
    don't_wait_for begin
      Instrument.shutdown () ;
      Logs_async.info (fun m -> m "Data server stopping") >>= fun () ->
      Shutdown.exit 0
    end
  end ;
  stage begin fun `Scheduler_started ->
    run ?start port no_pump (String.Set.of_list symbols)
  end

let () =
  let open Command.Let_syntax in
  Command.Staged.async ~summary:"Poloniex data aggregator"
    [%map_open
      let dry_run =
        flag "dry-run" no_arg ~doc:" Do not write trades in DBs"
      and no_pump =
        flag "no-pump" no_arg ~doc:" Do not pump trades"
      and start =
        flag "start" (optional date) ~doc:"date Start gathering history at DATE"
      and port =
        flag_optional_with_default_doc "port"
          int sexp_of_int ~default:5573 ~doc:"int TCP port to use"
      and datadir =
        flag_optional_with_default_doc "datadir" string String.sexp_of_t
          ~default:(Filename.concat "data" "poloniex")
          ~doc:"path Where to store DBs (data)"
      and () =
        Logs_async_reporter.set_level_via_param None
      and symbols =
        anon (sequence ("symbol" %: string)) in
      fun () ->
        main dry_run no_pump start port datadir symbols
    ]
  |> Command.run
