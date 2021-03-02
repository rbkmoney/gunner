%% @TODO More method wrappers, gun:headers support
%% @TODO Improve api to allow connection reusing for locking pools

-module(gunner).

%% API functions

-export([start_pool/1]).
-export([start_pool/2]).
-export([stop_pool/1]).

-export([pool_status/1]).
-export([pool_status/2]).

%% API Request wrappers

-export([get/3]).
-export([get/4]).
-export([get/5]).
-export([get/6]).

-export([post/4]).
-export([post/5]).
-export([post/6]).
-export([post/7]).

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

-type pool() :: gunner_pool:pool_pid().
-type pool_id() :: gunner_pool:pool_id().
-type pool_opts() :: gunner_pool:pool_opts().

-type end_host() :: inet:hostname() | inet:ip_address().
-type end_port() :: inet:port_number().
-type endpoint() :: {end_host(), end_port()}.

-opaque stream_ref() :: {connection_pid(), gun:stream_ref()}.

-type request_error() ::
    {resolve_failed, gunner_resolver:resolve_error()} |
    gunner_pool:acquire_error().

-export_type([pool/0]).
-export_type([pool_id/0]).
-export_type([pool_opts/0]).
-export_type([end_host/0]).
-export_type([end_port/0]).
-export_type([endpoint/0]).
-export_type([stream_ref/0]).

-export_type([request_error/0]).

%% Internal types

-type pool_reg_name() :: gunner_pool:pool_reg_name().

-type method() :: binary().
-type path() :: iodata().
-type req_headers() :: gun:req_headers().
-type body() :: iodata().
-type req_opts() :: gun:req_opts().

-type connection_pid() :: gunner_pool:connection_pid().

-type request_return() :: {ok, stream_ref()} | {error, request_error()}.

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

-define(DEFAULT_TIMEOUT, 1000).

%%
%% API functions
%%

-spec start_pool(pool_opts()) -> {ok, pool()} | {error, already_exists}.
start_pool(PoolOpts) ->
    gunner_pool:start_pool(PoolOpts).

-spec start_pool(pool_reg_name(), pool_opts()) -> {ok, pool()} | {error, already_exists}.
start_pool(PoolID, PoolOpts) ->
    gunner_pool:start_pool(PoolID, PoolOpts).

-spec stop_pool(pool()) -> ok | {error, not_found}.
stop_pool(Pool) ->
    gunner_pool:stop_pool(Pool).

-spec pool_status(pool_id()) -> {ok, gunner_pool:pool_status_response()} | {error, pool_not_found}.
pool_status(PoolID) ->
    pool_status(PoolID, ?DEFAULT_TIMEOUT).

-spec pool_status(pool_id(), timeout()) -> {ok, gunner_pool:pool_status_response()} | {error, pool_not_found}.
pool_status(PoolID, Timeout) ->
    gunner_pool:pool_status(PoolID, Timeout).

%%

-spec get(pool_id(), endpoint(), path()) -> request_return().
get(PoolID, Endpoint, Path) ->
    get(PoolID, Endpoint, Path, []).

-spec get(pool_id(), endpoint(), path(), req_headers()) -> request_return().
get(PoolID, Endpoint, Path, Headers) ->
    get(PoolID, Endpoint, Path, Headers, #{}).

-spec get(pool_id(), endpoint(), path(), req_headers(), req_opts()) -> request_return().
get(PoolID, Endpoint, Path, Headers, ReqOpts) ->
    request(PoolID, Endpoint, <<"GET">>, Path, Headers, <<>>, ReqOpts).

-spec get(pool_id(), endpoint(), path(), req_headers(), req_opts(), timeout()) -> request_return().
get(PoolID, Endpoint, Path, Headers, ReqOpts, AcquireTimeout) ->
    request(PoolID, Endpoint, <<"GET">>, Path, Headers, <<>>, ReqOpts, AcquireTimeout).

%%

-spec post(pool_id(), endpoint(), path(), body()) -> request_return().
post(PoolID, Endpoint, Path, Body) ->
    post(PoolID, Endpoint, Path, Body, []).

-spec post(pool_id(), endpoint(), path(), body(), req_headers()) -> request_return().
post(PoolID, Endpoint, Path, Body, Headers) ->
    post(PoolID, Endpoint, Path, Body, Headers, #{}).

-spec post(pool_id(), endpoint(), path(), body(), req_headers(), req_opts()) -> request_return().
post(PoolID, Endpoint, Path, Body, Headers, ReqOpts) ->
    request(PoolID, Endpoint, <<"POST">>, Path, Headers, Body, ReqOpts).

-spec post(pool_id(), endpoint(), path(), body(), req_headers(), req_opts(), timeout()) -> request_return().
post(PoolID, Endpoint, Path, Body, Headers, ReqOpts, AcquireTimeout) ->
    request(PoolID, Endpoint, <<"POST">>, Path, Headers, Body, ReqOpts, AcquireTimeout).

%%

-spec request(pool_id(), endpoint(), method(), path(), req_headers(), body(), req_opts()) -> request_return().
request(PoolID, Endpoint, Method, Path, Headers, Body, ReqOpts) ->
    request(PoolID, Endpoint, Method, Path, Headers, Body, ReqOpts, ?DEFAULT_TIMEOUT).

-spec request(pool_id(), endpoint(), method(), path(), req_headers(), body(), req_opts(), timeout()) ->
    request_return().
request(PoolID, Endpoint, Method, Path, Headers, Body, ReqOpts, AcquireTimeout) ->
    do_request(PoolID, Endpoint, Method, Path, Headers, Body, ReqOpts, AcquireTimeout).

%%

-spec free(pool_id(), stream_ref()) ->
    ok | {error, {invalid_pool_mode, loose} | connection_not_locked | connection_not_found}.
free(PoolID, GStreamRef) ->
    free(PoolID, GStreamRef, ?DEFAULT_TIMEOUT).

-spec free(pool_id(), stream_ref(), timeout()) ->
    ok | {error, {invalid_pool_mode, loose} | connection_not_locked | connection_not_found}.
free(PoolID, {ConnectionPid, _}, Timeout) ->
    gunner_pool:free(PoolID, ConnectionPid, Timeout).

%%

-spec await(stream_ref()) -> await_result().
await({ConnPid, StreamRef}) ->
    gun:await(ConnPid, StreamRef).

-spec await(stream_ref(), timeout()) -> await_result().
await({ConnPid, StreamRef}, Timeout) ->
    gun:await(ConnPid, StreamRef, Timeout).

-spec await_body(stream_ref()) -> await_body_result().
await_body({ConnPid, StreamRef}) ->
    gun:await_body(ConnPid, StreamRef).

-spec await_body(stream_ref(), timeout()) -> await_body_result().
await_body({ConnPid, StreamRef}, Timeout) ->
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

%%
%% Internal functions
%%

-spec do_request(pool_id(), endpoint(), method(), path(), req_headers(), body(), req_opts(), timeout()) ->
    request_return().
do_request(PoolID, Endpoint, Method, Path, Headers, Body, ReqOpts, AcquireTimeout) ->
    case acquire_connection(PoolID, Endpoint, AcquireTimeout) of
        {ok, ConnectionPid} ->
            StreamRef = gun:request(ConnectionPid, Method, Path, Headers, Body, ReqOpts),
            {ok, {ConnectionPid, StreamRef}};
        {error, _} = Error ->
            Error
    end.

acquire_connection(PoolID, Endpoint, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    case gunner_resolver:resolve_endpoint(Endpoint, #{timeout => Timeout}) of
        {ok, ResolvedEndpoint} ->
            TimeoutLeft = Deadline - erlang:monotonic_time(millisecond),
            gunner_pool:acquire(PoolID, ResolvedEndpoint, TimeoutLeft);
        {error, Reason} ->
            {error, {resolve_failed, Reason}}
    end.
