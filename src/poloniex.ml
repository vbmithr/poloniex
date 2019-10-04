open Core
open Async

open Plnx
open Poloniex_global
open Poloniex_util

module Rest = Plnx_rest
module Ws = Plnx_ws

module Encoding = Dtc_pb.Encoding
module DTC = Dtc_pb.Dtcprotocol_piqi

let _ = Clock_ns.every Time_ns.Span.day begin fun () ->
    Pair.Table.clear session_high ;
    Pair.Table.clear session_low ;
    Pair.Table.clear session_volume
  end

let failure_of_error e =
  match Error.to_exn e |> Monitor.extract_exn with
  | Failure msg -> Some msg
  | _ -> None

let main span timeout tls uid gid port sc =
  sc_mode := sc ;
  update_client_span := span ;
  stage begin fun `Scheduler_started ->
    let logs =
      Option.Monad_infix.(Sys.getenv "OVH_LOGS_URL" >>| Uri.of_string) in
    Logs_async_ovh.udp_reporter ?logs () >>= fun reporter ->
    Logs.set_reporter reporter ;
    let now = Time_ns.now () in
    Fastrest.request Rest.currencies >>| begin function
      | Error err -> Error.raise err
      | Ok currs ->
        List.iter currs ~f:(fun (c, t) -> String.Table.set currencies ~key:c ~data:t)
    end >>= fun () ->
    Fastrest.request Rest.tickers >>| begin function
      | Error err -> Error.raise err
      | Ok ts ->
        List.iter ts ~f:begin fun (pair, t) ->
          Pair.Table.add tickers pair (now, t)
        end
    end >>= fun () ->
    Restsync.Default.run () ;
    conduit_server ?tls () >>= fun server ->
    Option.iter uid ~f:Core.Unix.setuid ;
    Option.iter gid ~f:Core.Unix.setgid ;
    Poloniex_ws.launch
      (Poloniex_ws.bounded 10) ~timeout
      (Actor.Types.create_limits
         ?backlog_level:(Logs.Src.level src)
         ~backlog_size:100_000
         ()) "main" ()
      (module Poloniex_ws.Handlers) >>= fun _ws_worker ->
    Poloniex_dtc.launch
      (Poloniex_dtc.bounded 10)
      (Actor.Types.create_limits
         ?backlog_level:(Logs.Src.level src)
         ~backlog_size:50_000
         ()) "main" { server ; port }
      (module Poloniex_dtc.Handlers) >>= fun _dtc_worker ->
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
        Logs_async_reporter.set_level_via_param None
      and () =
        Logs_async_reporter.set_level_via_param
          ~arg_name:"local-log-level" (Some src) in
      fun () ->
        let tls =
          match crt, key with
          | Some crt, Some key -> Some (crt, key)
          | _ -> None in
        main client_span timeout tls uid gid port sc
    ]
  |> Command.run
