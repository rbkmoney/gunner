-module(gunner_default_event_h).

-behaviour(gunner_event_h).

%%

-export([handle_event/2]).

%%

-type state() :: gunner_event_h:state().

%%

-spec handle_event(gunner_event_h:event(), state()) -> state().
handle_event(_Event, State) ->
    State.
