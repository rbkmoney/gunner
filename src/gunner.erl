-module(gunner).

%% API functions

-export([start_pool/2]).
-export([start_pool/3]).

-export([stop_pool/1]).
-export([stop_pool/2]).

-export([pool_status/1]).
-export([pool_status/2]).

-export([acquire/2]).
-export([acquire/3]).

-export([free/2]).
-export([free/3]).

-export([transaction/3]).
-export([transaction/4]).

%% Application callbacks

-behaviour(application).

-export([start/2]).
-export([stop/1]).

%% API types

%% Internal types

-type pool_id() :: gunner_manager:pool_id().

-type pool() :: gunner_pool:pool().
-type pool_opts() :: gunner_pool:pool_opts().

-type worker() :: gunner_worker_factory:worker(_).
-type worker_args() :: gunner_worker_factory:worker_args(_).

-type transaction_fun() :: fun((worker()) -> transaction_result()).
-type transaction_result() :: any().

%%

-define(DEFAULT_TIMEOUT, 1000).

%%
%% API functions
%%

-spec start_pool(pool_id(), pool_opts()) -> ok | {error, already_exists}.
start_pool(PoolID, PoolOpts) ->
    start_pool(PoolID, PoolOpts, ?DEFAULT_TIMEOUT).

-spec start_pool(pool_id(), pool_opts(), timeout()) -> ok | {error, already_exists}.
start_pool(PoolID, PoolOpts, Timeout) ->
    gunner_manager:start_pool(PoolID, PoolOpts, Timeout).

-spec stop_pool(pool_id()) -> ok | {error, not_found}.
stop_pool(PoolID) ->
    stop_pool(PoolID, ?DEFAULT_TIMEOUT).

-spec stop_pool(pool_id(), timeout()) -> ok | {error, not_found}.
stop_pool(PoolID, Timeout) ->
    gunner_manager:stop_pool(PoolID, Timeout).

-spec pool_status(pool_id()) -> ok | {error, not_found}.
pool_status(PoolID) ->
    pool_status(PoolID, ?DEFAULT_TIMEOUT).

-spec pool_status(pool_id(), timeout()) -> ok | {error, not_found}.
pool_status(PoolID, Timeout) ->
    case get_pool(PoolID, Timeout) of
        {ok, Pool} ->
            {ok, get_pool_status(Pool, Timeout)};
        {error, not_found} ->
            {error, not_found}
    end.

%%

-spec acquire(pool_id(), worker_args()) ->
    {ok, worker()} | {error, pool_not_found | pool_unavailable | {worker_init_failed, _}}.
acquire(PoolID, WorkerArgs) ->
    acquire(PoolID, WorkerArgs, ?DEFAULT_TIMEOUT).

-spec acquire(pool_id(), worker_args(), timeout()) ->
    {ok, worker()} | {error, pool_not_found | pool_unavailable | {worker_init_failed, _}}.
acquire(PoolID, WorkerArgs, Timeout) ->
    case get_pool(PoolID, Timeout) of
        {ok, Pool} ->
            acquire_from_pool(Pool, WorkerArgs, Timeout);
        {error, not_found} ->
            {error, pool_not_found}
    end.

-spec free(pool_id(), worker()) -> ok | {error, pool_not_found | invalid_worker}.
free(PoolID, Worker) ->
    free(PoolID, Worker, ?DEFAULT_TIMEOUT).

-spec free(pool_id(), worker(), timeout()) -> ok | {error, pool_not_found | invalid_worker}.
free(PoolID, Worker, Timeout) ->
    case get_pool(PoolID, Timeout) of
        {ok, Pool} ->
            free_to_pool(Pool, Worker, Timeout);
        {error, not_found} ->
            {error, pool_not_found}
    end.

-spec transaction(pool_id(), worker_args(), transaction_fun()) ->
    transaction_result() | {error, pool_not_found | pool_unavailable | {worker_init_failed, _}}.
transaction(PoolID, WorkerArgs, TransactionFun) ->
    transaction(PoolID, WorkerArgs, TransactionFun, ?DEFAULT_TIMEOUT).

-spec transaction(pool_id(), worker_args(), transaction_fun(), timeout()) ->
    transaction_result() | {error, pool_not_found | pool_unavailable | {worker_init_failed, _}}.
transaction(PoolID, WorkerArgs, TransactionFun, Timeout) ->
    case acquire(PoolID, WorkerArgs, Timeout) of
        {ok, Worker} ->
            try
                TransactionFun(Worker)
            after
                ok = free(PoolID, Worker, Timeout)
            end;
        {error, _} = Error ->
            Error
    end.

%%
%% Application callbacks
%%

-spec start(normal, any()) -> {ok, pid()} | {error, any()}.
start(_StartType, _StartArgs) ->
    gunner_sup:start_link().

-spec stop(any()) -> ok.
stop(_State) ->
    ok.

%%
%% Internal functions
%%

-spec get_pool(pool_id(), timeout()) -> {ok, pool()} | {error, not_found}.
get_pool(PoolID, Timeout) ->
    gunner_manager:get_pool(PoolID, Timeout).

-spec acquire_from_pool(pool_id(), worker_args(), timeout()) ->
    {ok, worker()} | {error, pool_unavailable | {worker_init_failed, _}}.
acquire_from_pool(Pool, WorkerArgs, Timeout) ->
    try
        gunner_pool:acquire(Pool, WorkerArgs, Timeout)
    catch
        Error:Reason:Stacktrace ->
            gunner_pool:cancel_acquire(Pool),
            erlang:raise(Error, Reason, Stacktrace)
    end.

-spec free_to_pool(pool_id(), worker(), timeout()) -> ok | {error, invalid_worker}.
free_to_pool(Pool, Worker, Timeout) ->
    gunner_pool:free(Pool, Worker, Timeout).

-spec get_pool_status(pool_id(), timeout()) -> gunner_pool:status_response().
get_pool_status(Pool, Timeout) ->
    gunner_pool:pool_status(Pool, Timeout).
