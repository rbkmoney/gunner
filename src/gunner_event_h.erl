-module(gunner_event_h).

-include("gunner_events.hrl").

%% API

-export([handle_event/2]).

%% API Types

-type state() :: any().
-type handler() :: {module(), state()}.

-type event() ::
    pool_init_event() |
    pool_terminate_event() |
    acquire_started_event() |
    acquire_finished_event() |
    connection_locked_event() |
    connection_unlocked_event() |
    free_started_event() |
    free_finished_event() |
    free_error_event() |
    cleanup_started_event() |
    cleanup_finished_event() |
    client_down_event() |
    connection_init_started_event() |
    connection_init_finished_event() |
    connection_down_event().

-export_type([state/0]).
-export_type([handler/0]).
-export_type([event/0]).

%%

-type pool_init_event() :: #gunner_pool_init_event{}.
-type pool_terminate_event() :: #gunner_pool_terminate_event{}.

-type acquire_started_event() :: #gunner_acquire_started_event{}.
-type acquire_finished_event() :: #gunner_acquire_finished_event{}.

-type connection_locked_event() :: #gunner_connection_locked_event{}.
-type connection_unlocked_event() :: #gunner_connection_unlocked_event{}.

-type free_started_event() :: #gunner_free_started_event{}.
-type free_finished_event() :: #gunner_free_finished_event{}.
-type free_error_event() :: #gunner_free_error_event{}.

-type cleanup_started_event() :: #gunner_cleanup_started_event{}.
-type cleanup_finished_event() :: #gunner_cleanup_finished_event{}.

-type client_down_event() :: #gunner_client_down_event{}.

-type connection_init_started_event() :: #gunner_connection_init_started_event{}.
-type connection_init_finished_event() :: #gunner_connection_init_finished_event{}.

-type connection_down_event() :: #gunner_connection_down_event{}.

%%

-export_type([pool_init_event/0]).
-export_type([pool_terminate_event/0]).

-export_type([acquire_started_event/0]).
-export_type([acquire_finished_event/0]).

-export_type([connection_locked_event/0]).
-export_type([connection_unlocked_event/0]).

-export_type([free_started_event/0]).
-export_type([free_finished_event/0]).
-export_type([free_error_event/0]).

-export_type([cleanup_started_event/0]).
-export_type([cleanup_finished_event/0]).

-export_type([client_down_event/0]).

-export_type([connection_init_started_event/0]).
-export_type([connection_init_finished_event/0]).

-export_type([connection_down_event/0]).

%% Callbacks

-callback handle_event(event(), state()) -> state().

%% API

-spec handle_event(event(), handler()) -> handler().
handle_event(Event, {Handler, State0}) ->
    State1 = Handler:handle_event(Event, State0),
    {Handler, State1}.
