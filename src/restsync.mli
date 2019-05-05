open Async

type t

val create : unit -> t

val push : t -> (unit -> unit Deferred.t) -> unit Deferred.t
val push_nowait : t -> (unit -> unit Deferred.t) -> unit

val run : t -> unit

val start : t -> unit
val stop : t -> unit

val is_running : t -> bool

module Default : sig
  val push : (unit -> unit Deferred.t) -> unit Deferred.t
  val push_nowait : (unit -> unit Deferred.t) -> unit
  val run : unit -> unit
  val start : unit -> unit
  val stop : unit -> unit
  val is_running : unit -> bool
end
