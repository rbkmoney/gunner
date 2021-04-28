-module(gunner_event_h).

-include("gunner_events.h").

%% API

-export([handle_event/2]).

%% API Types

-type state() :: any().
-type handler() :: {module(), state()}.

-type event() ::
    init_event() |
    acquire_started_event() |
    pool_starvation_event() |
    acquire_finished_event() |
    free_started_event() |
    free_finished_event() |
    cleanup_started_event() |
    cleanup_finished_event() |
    client_down_event() |
    connection_down_event() |
    terminate_event().

-export_type([state/0]).
-export_type([handler/0]).
-export_type([event/0]).

%%

-type init_event() :: #gunner_init_event{}.
-type acquire_started_event() :: #gunner_acquire_started_event{}.
-type pool_starvation_event() :: #gunner_pool_starvation_event{}.
-type acquire_finished_event() :: #gunner_acquire_finished_event{}.
-type free_started_event() :: #gunner_free_started_event{}.
-type free_finished_event() :: #gunner_free_finished_event{}.
-type cleanup_started_event() :: #gunner_cleanup_started_event{}.
-type cleanup_finished_event() :: #gunner_cleanup_finished_event{}.
-type client_down_event() :: #gunner_client_down_event{}.
-type connection_down_event() :: #gunner_connection_down_event{}.
-type terminate_event() :: #gunner_terminate_event{}.

%%

-export_type([init_event/0]).
-export_type([acquire_started_event/0]).
-export_type([pool_starvation_event/0]).
-export_type([acquire_finished_event/0]).
-export_type([free_started_event/0]).
-export_type([free_finished_event/0]).
-export_type([cleanup_started_event/0]).
-export_type([cleanup_finished_event/0]).
-export_type([client_down_event/0]).
-export_type([connection_down_event/0]).
-export_type([terminate_event/0]).

%% Callbacks

-callback handle_event(event(), state()) -> state().

%% API

-spec handle_event(event(), handler()) -> handler().
handle_event(Event, {Handler, State0}) ->
    State1 = Handler:handle_event(Event, State0),
    {Handler, State1}.
