-module(gunner).

%% API functions

-export([start_pool/2]).
-export([stop_pool/1]).

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

-type worker() :: pid().
-type worker_args() :: {conn_host(), conn_port()}.

-export_type([worker/0]).
-export_type([worker_args/0]).

%% Internal types

-type pool_id() :: gunner_pool:pool_id().
-type pool_opts() :: gunner_pool:pool_opts().

-type conn_host() :: inet:hostname() | inet:ip_address().
-type conn_port() :: inet:port_number().

-type transaction_fun() :: fun((worker()) -> transaction_result()).
-type transaction_result() :: any().

%%

-define(DEFAULT_TIMEOUT, 1000).

%%
%% API functions
%%

-spec start_pool(pool_id(), pool_opts()) -> ok | {error, already_exists}.
start_pool(PoolID, PoolOpts) ->
    gunner_pool:start_pool(PoolID, PoolOpts).

-spec stop_pool(pool_id()) -> ok | {error, not_found}.
stop_pool(PoolID) ->
    gunner_pool:stop_pool(PoolID).

-spec pool_status(pool_id()) -> {ok, gunner_pool:status_response()} | {error, pool_not_found}.
pool_status(PoolID) ->
    pool_status(PoolID, ?DEFAULT_TIMEOUT).

-spec pool_status(pool_id(), timeout()) -> {ok, gunner_pool:status_response()} | {error, pool_not_found}.
pool_status(PoolID, Timeout) ->
    gunner_pool:pool_status(PoolID, Timeout).

%%

-spec acquire(pool_id(), worker_args()) ->
    {ok, worker()} | {error, pool_not_found | pool_unavailable | {worker_init_failed, _}} | no_return().
acquire(PoolID, WorkerArgs) ->
    acquire(PoolID, WorkerArgs, ?DEFAULT_TIMEOUT).

-spec acquire(pool_id(), worker_args(), timeout()) ->
    {ok, worker()} | {error, pool_not_found | pool_unavailable | {worker_init_failed, _}} | no_return().
acquire(PoolID, WorkerArgs, Timeout) ->
    try
        gunner_pool:acquire(PoolID, WorkerArgs, Timeout)
    catch
        Error:Reason:Stacktrace ->
            ok = gunner_pool:cancel_acquire(PoolID),
            erlang:raise(Error, Reason, Stacktrace)
    end.

-spec free(pool_id(), worker()) -> ok | {error, pool_not_found | invalid_worker}.
free(PoolID, Worker) ->
    free(PoolID, Worker, ?DEFAULT_TIMEOUT).

-spec free(pool_id(), worker(), timeout()) -> ok | {error, pool_not_found | invalid_worker}.
free(PoolID, Worker, Timeout) ->
    gunner_pool:free(PoolID, Worker, Timeout).

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
