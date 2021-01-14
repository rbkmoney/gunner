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

-type connection() :: pid().
-type connection_args() :: {conn_host(), conn_port()}.

-export_type([connection/0]).
-export_type([connection_args/0]).

%% Internal types

-type pool_id() :: gunner_pool:pool_id().
-type pool_opts() :: gunner_pool:pool_opts().

-type conn_host() :: inet:hostname() | inet:ip_address().
-type conn_port() :: inet:port_number().

-type transaction_fun() :: fun((connection()) -> transaction_result()).
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

-spec acquire(pool_id(), connection_args()) ->
    {ok, connection()} | {error, pool_not_found | pool_unavailable | {connection_init_failed, _}} | no_return().
acquire(PoolID, ConnectionArgs) ->
    acquire(PoolID, ConnectionArgs, ?DEFAULT_TIMEOUT).

-spec acquire(pool_id(), connection_args(), timeout()) ->
    {ok, connection()} | {error, pool_not_found | pool_unavailable | {connection_init_failed, _}} | no_return().
acquire(PoolID, ConnectionArgs, Timeout) ->
    Ticket = erlang:make_ref(),
    try
        gunner_pool:acquire(PoolID, ConnectionArgs, Ticket, Timeout)
    catch
        Error:Reason:Stacktrace ->
            ok = gunner_pool:cancel_acquire(PoolID, Ticket),
            erlang:raise(Error, Reason, Stacktrace)
    end.

-spec free(pool_id(), connection()) -> ok | {error, pool_not_found | invalid_connection}.
free(PoolID, Connection) ->
    free(PoolID, Connection, ?DEFAULT_TIMEOUT).

-spec free(pool_id(), connection(), timeout()) -> ok | {error, pool_not_found | invalid_connection}.
free(PoolID, Connection, Timeout) ->
    gunner_pool:free(PoolID, Connection, Timeout).

-spec transaction(pool_id(), connection_args(), transaction_fun()) ->
    transaction_result() | {error, pool_not_found | pool_unavailable | {connection_init_failed, _}}.
transaction(PoolID, ConnectionArgs, TransactionFun) ->
    transaction(PoolID, ConnectionArgs, TransactionFun, ?DEFAULT_TIMEOUT).

-spec transaction(pool_id(), connection_args(), transaction_fun(), timeout()) ->
    transaction_result() | {error, pool_not_found | pool_unavailable | {connection_init_failed, _}}.
transaction(PoolID, ConnectionArgs, TransactionFun, Timeout) ->
    case acquire(PoolID, ConnectionArgs, Timeout) of
        {ok, Connection} ->
            try
                TransactionFun(Connection)
            after
                ok = free(PoolID, Connection, Timeout)
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
