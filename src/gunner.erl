%% @TODO More method wrappers, gun:headers support
%% @TODO Improve api to allow connection reusing for locking pools

-module(gunner).

%% API functions

-export([start_pool/1]).
-export([start_pool/2]).
-export([stop_pool/1]).

%% API Synchronous requests

-export([get/3]).
-export([get/4]).
-export([get/5]).

-export([post/4]).
-export([post/5]).
-export([post/6]).

-export([request/7]).

%% API Locking transaction

-export([transaction/4]).

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

-type opts() :: #{
    req_opts => gun:req_opts(),
    acquire_timeout => timeout(),
    request_timeout => timeout()
}.

-type response() ::
    {ok, http_code(), response_headers(), response_body() | undefined} |
    {error, {unknown, _}}.

-type http_code() :: 200..599.
-type response_headers() :: [{binary(), binary()}].
-type response_body() :: binary().

-type acquire_error() :: gunner_pool:acquire_error().

-export_type([pool/0]).
-export_type([pool_id/0]).
-export_type([pool_opts/0]).
-export_type([end_host/0]).
-export_type([end_port/0]).
-export_type([endpoint/0]).
-export_type([opts/0]).

-export_type([response/0]).
-export_type([http_code/0]).
-export_type([response_headers/0]).
-export_type([response_body/0]).

-export_type([acquire_error/0]).

%% Internal types

-type pool_reg_name() :: gunner_pool:pool_reg_name().

-type method() :: binary().
-type path() :: iodata().
-type req_headers() :: gun:req_headers().
-type body() :: iodata().

-type connection_pid() :: gunner_pool:connection_pid().

-type transaction_fun(Ret) :: fun((connection_pid()) -> Ret).

-define(DEFAULT_TIMEOUT, 1000).

%%
%% API functions
%%

-spec start_pool(pool_opts()) -> {ok, pool()} | {error, {pool, already_exists}}.
start_pool(PoolOpts) ->
    gunner_pool:start_pool(PoolOpts).

-spec start_pool(pool_reg_name(), pool_opts()) -> {ok, pool()} | {error, {pool, already_exists}}.
start_pool(PoolID, PoolOpts) ->
    gunner_pool:start_pool(PoolID, PoolOpts).

-spec stop_pool(pool()) -> ok | {error, {pool, not_found}}.
stop_pool(Pool) ->
    gunner_pool:stop_pool(Pool).

%%

-spec get(pool_id(), endpoint(), path()) -> response() | {error, acquire_error()}.
get(PoolID, Endpoint, Path) ->
    get(PoolID, Endpoint, Path, []).

-spec get(pool_id(), endpoint(), path(), req_headers()) -> response() | {error, acquire_error()}.
get(PoolID, Endpoint, Path, Headers) ->
    get(PoolID, Endpoint, Path, Headers, #{}).

-spec get(pool_id(), endpoint(), path(), req_headers(), opts()) -> response() | {error, acquire_error()}.
get(PoolID, Endpoint, Path, Headers, Opts) ->
    request(PoolID, Endpoint, <<"GET">>, Path, Headers, <<>>, Opts).

%%

-spec post(pool_id(), endpoint(), path(), body()) -> response() | {error, acquire_error()}.
post(PoolID, Endpoint, Path, Body) ->
    post(PoolID, Endpoint, Path, Body, []).

-spec post(pool_id(), endpoint(), path(), body(), req_headers()) -> response() | {error, acquire_error()}.
post(PoolID, Endpoint, Path, Body, Headers) ->
    post(PoolID, Endpoint, Path, Body, Headers, #{}).

-spec post(pool_id(), endpoint(), path(), body(), req_headers(), opts()) -> response() | {error, acquire_error()}.
post(PoolID, Endpoint, Path, Body, Headers, Opts) ->
    request(PoolID, Endpoint, <<"POST">>, Path, Headers, Body, Opts).

%%

-spec request(pool_id(), endpoint(), method(), path(), req_headers(), body(), opts()) ->
    response() | {error, acquire_error()}.
request(PoolID, Endpoint, Method, Path, Headers, Body, Opts) ->
    Timeout = maps:get(acquire_timeout, Opts, ?DEFAULT_TIMEOUT),
    case gunner_pool:acquire(PoolID, Endpoint, false, Timeout) of
        {ok, ConnectionPid} ->
            StreamRef = gun:request(ConnectionPid, Method, Path, Headers, Body, maps:get(req_opts, Opts, #{})),
            await_response(ConnectionPid, StreamRef, Opts);
        {error, _} = Error ->
            Error
    end.

%%

-spec transaction(pool_id(), endpoint(), opts(), transaction_fun(Ret)) -> {ok, Ret} | {error, acquire_error()}.
transaction(PoolID, Endpoint, Opts, Fun) ->
    Timeout = maps:get(acquire_timeout, Opts, ?DEFAULT_TIMEOUT),
    case gunner_pool:acquire(PoolID, Endpoint, true, Timeout) of
        {ok, ConnectionPid} ->
            try
                Result = Fun(ConnectionPid),
                {ok, Result}
            after
                ok = gunner_pool:free(PoolID, ConnectionPid)
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

%% Internal functions

await_response(ConnectionPid, StreamRef, Opts) ->
    Timeout = maps:get(request_timeout, Opts, ?DEFAULT_TIMEOUT),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    case gun:await(ConnectionPid, StreamRef, Timeout) of
        {response, nofin, Code, Headers} ->
            TimeoutLeft = Deadline - erlang:monotonic_time(millisecond),
            case gun:await_body(ConnectionPid, StreamRef, TimeoutLeft) of
                {ok, Body, _Trailers} ->
                    {ok, Code, Headers, Body};
                {ok, Body} ->
                    {ok, Code, Headers, Body};
                {error, Reason} ->
                    {error, {unknown, Reason}}
            end;
        {response, fin, Code, Headers} ->
            {ok, Code, Headers, undefined};
        {error, Reason} ->
            {error, {unknown, Reason}}
    end.
