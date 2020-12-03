-module(gunner_worker_factory).

%% API

-export([on_acquire/2]).

-export([create_worker/2]).
-export([exit_worker/2]).

%% Behavour

-callback on_acquire(worker_args(_)) -> {worker_group_id(), worker_args(_)}.
-callback create_worker(worker_args(_)) -> {ok, worker(_)} | {error, Reason :: _}.
-callback exit_worker(worker(_)) -> ok.

%% API Types

-type worker(A) :: A.
-type worker_args(A) :: A.

-export_type([worker/1]).
-export_type([worker_args/1]).

%% Internal types

-type worker_group_id() :: gunner_pool:worker_group_id().
-type handler() :: module().

%%
%% API functions
%%

-spec on_acquire(handler(), worker_args(_)) -> {worker_group_id(), worker_args(_)}.
on_acquire(Handler, WorkerArgs) ->
    Handler:on_acquire(WorkerArgs).

-spec create_worker(handler(), worker_args(_)) -> {ok, worker(_)} | {error, Reason :: _}.
create_worker(Handler, WorkerArgs) ->
    Handler:create_worker(WorkerArgs).

-spec exit_worker(handler(), worker(_)) -> ok.
exit_worker(Handler, Worker) ->
    Handler:exit_worker(Worker).
