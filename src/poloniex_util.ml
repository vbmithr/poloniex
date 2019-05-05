open Core
open Async

open Poloniex_global

let pp_print_addr ppf t =
  Format.fprintf ppf "%a" Sexplib.Sexp.pp
    (Socket.Address.Inet.sexp_of_t t)

let loop_log_errors f =
  let rec inner () =
    Monitor.try_with_or_error ~name:"loop_log_errors" f >>= function
    | Ok _ -> assert false
    | Error err ->
      Log_async.err begin fun m ->
        m "run: %a" Error.pp err
      end >>= fun () ->
      inner ()
  in inner ()

let conduit_server ?tls () =
  match tls with
  | None -> return `TCP
  | Some (crt, key) ->
    Sys.file_exists crt >>= fun crt_exists ->
    Sys.file_exists key >>| fun key_exists ->
    match crt_exists, key_exists with
    | `Yes, `Yes -> `OpenSSL (`Crt_file_path crt, `Key_file_path key)
    | _ -> failwith "TLS crt/key file not found"
