open Async

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

let push { w ; _ } thunk =
  Pipe.write w thunk

let push_nowait { w ; _ } thunk =
  Pipe.write_without_pushback w thunk

let run { r ; run ; condition ; _ } =
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
let is_running { run ; _ } = run

module Default = struct
  let default = create ()

  let push thunk = push default thunk
  let push_nowait thunk = push_nowait default thunk

  let run () = run default
  let start () = start default
  let stop () = stop default
  let is_running () = is_running default
end
