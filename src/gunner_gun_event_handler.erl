-module(gunner_gun_event_handler).

-behavior(gun_event).

-export([init/2]).
-export([domain_lookup_start/2]).
-export([domain_lookup_end/2]).
-export([connect_start/2]).
-export([connect_end/2]).
-export([tls_handshake_start/2]).
-export([tls_handshake_end/2]).
-export([request_start/2]).
-export([request_headers/2]).
-export([request_end/2]).
-export([push_promise_start/2]).
-export([push_promise_end/2]).
-export([response_start/2]).
-export([response_inform/2]).
-export([response_headers/2]).
-export([response_trailers/2]).
-export([response_end/2]).
-export([ws_upgrade/2]).
-export([ws_recv_frame_start/2]).
-export([ws_recv_frame_header/2]).
-export([ws_recv_frame_end/2]).
-export([ws_send_frame_start/2]).
-export([ws_send_frame_end/2]).
-export([protocol_changed/2]).
-export([origin_changed/2]).
-export([cancel/2]).
-export([disconnect/2]).
-export([terminate/2]).

-type state() :: #{
    counters_ref := counters:counters_ref(),
    counters_idx := gunner_idx_authority:idx(),
    streams := streams(),
    % Wrapped event handler
    event_handler => {module(), State :: any()}
}.

-type streams() :: #{
    stream_ref() => {RequestEnded :: boolean(), ResponseEnded :: boolean()}
}.

-type stream_ref() :: gun:stream_ref().

-spec init(gun_event:init_event(), state()) -> state().
init(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec domain_lookup_start(gun_event:domain_lookup_event(), state()) -> state().
domain_lookup_start(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec domain_lookup_end(gun_event:domain_lookup_event(), state()) -> state().
domain_lookup_end(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec connect_start(gun_event:connect_event(), state()) -> state().
connect_start(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec connect_end(gun_event:connect_event(), state()) -> state().
connect_end(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec tls_handshake_start(gun_event:tls_handshake_event(), state()) -> state().
tls_handshake_start(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec tls_handshake_end(gun_event:tls_handshake_event(), state()) -> state().
tls_handshake_end(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec request_start(gun_event:request_start_event(), state()) -> state().
request_start(Event, State = #{streams := Streams0, counters_ref := CountersRef, counters_idx := Idx}) ->
    StreamRef = maps:get(stream_ref, Event),
    ok = counters:add(CountersRef, Idx, 1),
    Streams1 = Streams0#{StreamRef => {false, false}},
    forward_handler(Event, State#{streams => Streams1}, ?FUNCTION_NAME).

-spec request_headers(gun_event:request_start_event(), state()) -> state().
request_headers(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec request_end(gun_event:request_end_event(), state()) -> state().
request_end(Event, State = #{streams := Streams0, counters_ref := CountersRef, counters_idx := Idx}) ->
    StreamRef = maps:get(stream_ref, Event),
    Streams1 =
        case maps:get(StreamRef, Streams0) of
            {false, true} ->
                ok = counters:sub(CountersRef, Idx, 1),
                maps:remove(StreamRef, Streams0);
            {false, false} ->
                Streams0#{StreamRef => {true, false}}
        end,
    forward_handler(Event, State#{streams => Streams1}, ?FUNCTION_NAME).

-spec push_promise_start(gun_event:push_promise_start_event(), state()) -> state().
push_promise_start(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec push_promise_end(gun_event:push_promise_end_event(), state()) -> state().
push_promise_end(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec response_start(gun_event:response_start_event(), state()) -> state().
response_start(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec response_inform(gun_event:response_headers_event(), state()) -> state().
response_inform(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec response_headers(gun_event:response_headers_event(), state()) -> state().
response_headers(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec response_trailers(gun_event:response_trailers_event(), state()) -> state().
response_trailers(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec response_end(gun_event:response_end_event(), state()) -> state().
response_end(Event, State = #{streams := Streams0, counters_ref := CountersRef, counters_idx := Idx}) ->
    StreamRef = maps:get(stream_ref, Event),
    Streams1 =
        case maps:get(StreamRef, Streams0) of
            {true, false} ->
                ok = counters:sub(CountersRef, Idx, 1),
                maps:remove(StreamRef, Streams0);
            {false, false} ->
                Streams0#{StreamRef => {false, true}}
        end,
    forward_handler(Event, State#{streams => Streams1}, ?FUNCTION_NAME).

-spec ws_upgrade(gun_event:ws_upgrade_event(), state()) -> state().
ws_upgrade(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec ws_recv_frame_start(gun_event:ws_recv_frame_start_event(), state()) -> state().
ws_recv_frame_start(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec ws_recv_frame_header(gun_event:ws_recv_frame_header_event(), state()) -> state().
ws_recv_frame_header(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec ws_recv_frame_end(gun_event:ws_recv_frame_end_event(), state()) -> state().
ws_recv_frame_end(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec ws_send_frame_start(gun_event:ws_send_frame_event(), state()) -> state().
ws_send_frame_start(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec ws_send_frame_end(gun_event:ws_send_frame_event(), state()) -> state().
ws_send_frame_end(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec protocol_changed(gun_event:protocol_changed_event(), state()) -> state().
protocol_changed(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec origin_changed(gun_event:origin_changed_event(), state()) -> state().
origin_changed(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec cancel(gun_event:cancel_event(), state()) -> state().
cancel(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec disconnect(gun_event:disconnect_event(), state()) -> state().
disconnect(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec terminate(gun_event:terminate_event(), state()) -> state().
terminate(Event, State) ->
    forward_handler(Event, State, ?FUNCTION_NAME).

-spec forward_handler(Event :: _, state(), Fun :: atom()) -> state().
forward_handler(Event, State = #{event_handler := {Mod, ModState0}}, Fun) ->
    ModState = Mod:Fun(Event, ModState0),
    State#{event_handler => {Mod, ModState}};
forward_handler(_, State, _) ->
    State.
