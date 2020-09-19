module type ActorModel = sig
  type ('a, 'x) actor_thread

  val ( >>= ) :
    ('a, 'x) actor_thread ->
    ('x -> ('a, 'y) actor_thread) ->
    ('a, 'y) actor_thread

  val result : 'x -> ('a, 'x) actor_thread

  val receive : ('a, 'a) actor_thread

  type 'a actor

  val start : ('a, unit) actor_thread -> 'a actor

  val self : ('a, 'a actor) actor_thread

  val send : 'a actor -> 'a -> unit
end

module type HopacModel = sig
  type 'x job

  val ( >>= ) : 'x job -> ('x -> 'y job) -> 'y job

  val result : 'x -> 'x job

  val start : unit job -> unit

  type 'x ch

  val chan : unit -> 'x ch

  val give : 'x ch -> 'x -> unit job

  val take : 'x ch -> 'x job
end

module Actor (H : HopacModel) : ActorModel = struct
  type ('a, 'x) actor_thread = AT of ('a H.ch -> 'x H.job)

  let un_at (AT x) = x

  let ( >>= ) xa xa_to_ya =
    let ( >>= ) = H.( >>= ) in
    AT (fun a_ch -> un_at xa a_ch >>= fun x -> un_at (xa_to_ya x) a_ch)

  let result x = AT (fun a_ch -> H.result x)

  let receive = AT (fun a_ch -> Obj.magic @@ H.take a_ch)

  type 'a actor = A of 'a H.ch

  let un_a (A a_ch) = a_ch

  let start u_a =
    let a_ch = H.chan () in
    H.start (un_at u_a a_ch);
    A a_ch

  let self = AT (fun a_ch -> H.result @@ A a_ch)

  let send a_a a = H.start @@ H.give (un_a a_a) a
end

module Hopac (H : HopacModel) : HopacModel = struct
  module A = Actor(H)

  type 'x job = J of (unit, 'x) A.actor_thread

  let un_j (J x) = x

  type 'x msg = Take of unit A.actor * 'x option ref | Give of unit A.actor * 'x

  type 'x ch = C of 'x msg A.actor

  let un_c (C x) = x

  let chan () =
    let r = ref None in
    let givers = Queue.create () in
    let takers = Queue.create () in
    let ( >>= ) = A.( >>= ) in
    let rec loop () =
      A.receive >>= function
      | Give (giver, x) ->
          if Queue.length takers > 0 then (
            let taker, r = Queue.pop takers in
            r := Some x;
            A.send giver ();
            A.send taker ();
            loop () )
          else (
            Queue.push (giver, x) givers;
            loop () )
      | Take (taker, r) ->
          if Queue.length givers > 0 then (
            let giver, x = Queue.pop givers in
            r := Some x;
            A.send giver ();
            A.send taker ();
            loop () )
          else (
            Queue.push (taker, r) takers;
            loop () )
    in
    C (A.start @@ loop ())

  let give x_ch x =
    let ( >>= ) = A.( >>= ) in
    J
      ( A.self >>= fun giver ->
        A.send (un_c x_ch) (Give (giver, x));
        A.receive )

  let take x_ch =
    let ( >>= ) = A.( >>= ) in
    J
      ( A.self >>= fun taker ->
        let r = ref None in
        A.send (un_c x_ch) (Take (taker, r));
        A.receive >>= fun () ->
        match !r with None -> failwith "Impossible" | Some x -> A.result x )

  let ( >>= ) x_j x_to_yj = 
    let ( >>= ) = A.( >>= ) in 
    J (un_j x_j >>= fun x -> un_j (x_to_yj x))

  let result x = J (A.result x)

  let start u_j = A.start (un_j u_j) |> ignore
end