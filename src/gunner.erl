%% @TODO More method wrappers, gun:headers support
%% @TODO Improve api to allow connection reusing for locking pools

-module(gunner).

%% API functions

-export([start_pool/2]).
-export([stop_pool/1]).

-export([pool_status/1]).
-export([pool_status/2]).

%% API Request wrappers

-export([get/4]).
-export([get/5]).
-export([get/6]).

-export([post/6]).
-export([post/7]).

-export([request/6]).
-export([request/7]).
-export([request/8]).

%% API Await wrappers

-export([await/1]).
-export([await/2]).

-export([await_body/1]).
-export([await_body/2]).

%% API Locking mode functions

-export([free/2]).
-export([free/3]).

%% Application callbacks

-behaviour(application).

-export([start/2]).
-export([stop/1]).

%% API types

-type connection_args() :: {conn_host(), conn_port()}.

-opaque gunner_stream_ref() :: {gunner_ref, connection_pid(), gun:stream_ref()}.

-export_type([connection_args/0]).
-export_type([gunner_stream_ref/0]).

%% Internal types

-type pool_id() :: gunner_pool:pool_id().
-type pool_opts() :: gunner_pool:pool_opts().

-type conn_host() :: inet:hostname() | inet:ip_address().
-type conn_port() :: inet:port_number().

-type method() :: binary().
-type path() :: iodata().
-type req_headers() :: gun:req_headers().
-type body() :: iodata().
-type req_opts() :: gun:req_opts().

-type connection_pid() :: gunner_pool:connection_pid().

-type request_return() ::
    {ok, gunner_stream_ref()} | {error, acquire_error()}.

-type acquire_error() ::
    pool_not_found | pool_unavailable | {failed_to_start_connection, Why :: _} | {connection_failed, Why :: _}.

%% Copypasted from gun.erl
-type resp_headers() :: [{binary(), binary()}].
-type await_result() ::
    {inform, 100..199, resp_headers()} |
    {response, fin | nofin, non_neg_integer(), resp_headers()} |
    {data, fin | nofin, binary()} |
    {sse, cow_sse:event() | fin} |
    {trailers, resp_headers()} |
    {push, gun:stream_ref(), binary(), binary(), resp_headers()} |
    {upgrade, [binary()], resp_headers()} |
    {ws, gun:ws_frame()} |
    {up, http | http2 | raw | socks} |
    {notify, settings_changed, map()} |
    {error, {stream_error | connection_error | down, any()} | timeout}.

-type await_body_result() ::
    {ok, binary()} |
    {ok, binary(), resp_headers()} |
    {error, {stream_error | connection_error | down, any()} | timeout}.

%%

-define(DEFAULT_POOL, default).
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

-spec pool_status(pool_id()) -> {ok, gunner_pool:pool_status_response()} | {error, pool_not_found}.
pool_status(PoolID) ->
    pool_status(PoolID, ?DEFAULT_TIMEOUT).

-spec pool_status(pool_id(), timeout()) -> {ok, gunner_pool:pool_status_response()} | {error, pool_not_found}.
pool_status(PoolID, Timeout) ->
    gunner_pool:pool_status(PoolID, Timeout).

%%

-spec get(pool_id(), connection_args(), path(), timeout()) -> request_return().
get(PoolID, ConnectionArgs, Path, Timeout) ->
    request(PoolID, ConnectionArgs, <<"GET">>, Path, [], <<>>, #{}, Timeout).

-spec get(pool_id(), connection_args(), path(), req_headers(), timeout()) -> request_return().
get(PoolID, ConnectionArgs, Path, Headers, Timeout) ->
    request(PoolID, ConnectionArgs, <<"GET">>, Path, Headers, <<>>, #{}, Timeout).

-spec get(pool_id(), connection_args(), path(), req_headers(), req_opts(), timeout()) -> request_return().
get(PoolID, ConnectionArgs, Path, Headers, ReqOpts, Timeout) ->
    request(PoolID, ConnectionArgs, <<"GET">>, Path, Headers, <<>>, ReqOpts, Timeout).

%%

-spec post(pool_id(), connection_args(), path(), req_headers(), body(), timeout()) -> request_return().
post(PoolID, ConnectionArgs, Path, Headers, Body, Timeout) ->
    request(PoolID, ConnectionArgs, <<"POST">>, Path, Headers, Body, #{}, Timeout).

-spec post(pool_id(), connection_args(), path(), req_headers(), body(), req_opts(), timeout()) -> request_return().
post(PoolID, ConnectionArgs, Path, Headers, Body, ReqOpts, Timeout) ->
    request(PoolID, ConnectionArgs, <<"POST">>, Path, Headers, Body, ReqOpts, Timeout).

%%

%%

-spec request(connection_args(), method(), path(), req_headers(), body(), req_opts()) -> request_return().
request(ConnectionArgs, Method, Path, Headers, Body, ReqOpts) ->
    request(?DEFAULT_POOL, ConnectionArgs, Method, Path, Headers, Body, ReqOpts).

-spec request(pool_id(), connection_args(), method(), path(), req_headers(), body(), req_opts()) -> request_return().
request(PoolID, ConnectionArgs, Method, Path, Headers, Body, ReqOpts) ->
    request(PoolID, ConnectionArgs, Method, Path, Headers, Body, ReqOpts, ?DEFAULT_TIMEOUT).

-spec request(pool_id(), connection_args(), method(), path(), req_headers(), body(), req_opts(), timeout()) ->
    request_return().
request(PoolID, ConnectionArgs, Method, Path, Headers, Body, ReqOpts, Timeout) ->
    case gunner_pool:acquire(PoolID, ConnectionArgs, Timeout) of
        {ok, ConnectionPid} ->
            StreamRef = gun:request(ConnectionPid, Method, Path, Headers, Body, ReqOpts),
            {ok, {gunner_ref, ConnectionPid, StreamRef}};
        {error, _Reason} = Error ->
            Error
    end.

%%

-spec free(pool_id(), gunner_stream_ref()) ->
    ok | {error, {invalid_pool_mode, loose} | invalid_connection_state | connection_not_found}.
free(PoolID, GStreamRef) ->
    free(PoolID, GStreamRef, ?DEFAULT_TIMEOUT).

-spec free(pool_id(), gunner_stream_ref(), timeout()) ->
    ok | {error, {invalid_pool_mode, loose} | invalid_connection_state | connection_not_found}.
free(PoolID, {gunner_ref, ConnectionPid, _}, Timeout) ->
    gunner_pool:free(PoolID, ConnectionPid, Timeout).

%%

-spec await(gunner_stream_ref()) -> await_result().
await({gunner_ref, ConnPid, StreamRef}) ->
    gun:await(ConnPid, StreamRef).

-spec await(gunner_stream_ref(), timeout()) -> await_result().
await({gunner_ref, ConnPid, StreamRef}, Timeout) ->
    gun:await(ConnPid, StreamRef, Timeout).

-spec await_body(gunner_stream_ref()) -> await_body_result().
await_body({gunner_ref, ConnPid, StreamRef}) ->
    gun:await_body(ConnPid, StreamRef).

-spec await_body(gunner_stream_ref(), timeout()) -> await_body_result().
await_body({gunner_ref, ConnPid, StreamRef}, Timeout) ->
    gun:await_body(ConnPid, StreamRef, Timeout).

%%
%% Application callbacks
%%

-spec start(normal, any()) -> {ok, pid()} | {error, any()}.
start(_StartType, _StartArgs) ->
    gunner_sup:start_link().

-spec stop(any()) -> ok.
stop(_State) ->
    ok.
