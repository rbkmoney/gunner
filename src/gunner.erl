-module(gunner).

%% API functions

-export([request/9]).

%% Application callbacks

-behaviour(application).

-export([start/2]).
-export([stop/1]).

%% API types

-type pool_id() :: binary().
-type conn_host() :: inet:hostname() | inet:ip_address().
-type conn_port() :: inet:port_number().
-type req_path() :: iodata().
-type req_method() :: binary().
-type req_headers() :: gun:req_headers().
-type req_opts() :: gun:req_opts().
-type body() :: binary().

-type response() :: gunner_connection:response_data().

-export_type([
    pool_id/0,
    conn_host/0,
    conn_port/0,
    req_path/0,
    req_method/0,
    req_headers/0,
    req_opts/0,
    body/0,
    response/0
]).

%% Internal types
-type timeout_t() :: {Now :: erlang:timestamp(), RemainingMS :: integer()}.

-type pool() :: gunner_pool:pool().
-type connection() :: gunner_connection:connection().

%%
%% API functions
%%

-spec request(
    pool_id(),
    conn_host(),
    conn_port(),
    req_path(),
    req_method(),
    req_headers(),
    body(),
    req_opts(),
    timeout()
) -> {ok, response()} | {error, _Reason}.
request(PoolID, Host, Port, Path, Method, ReqHeaders, Body, ReqOpts, TimeoutMS) ->
    Timeout0 = init_timeout(TimeoutMS),
    case get_pool(PoolID, Timeout0) of
        {ok, Pool} ->
            Timeout1 = calculate_next_timeout(Timeout0),
            case acquire_connection(Pool, Host, Port, Timeout1) of
                {ok, Connection} ->
                    Timeout2 = calculate_next_timeout(Timeout1),
                    Result = do_request(Connection, Path, Method, ReqHeaders, Body, ReqOpts, Timeout2),
                    ok = gunner_pool:free_connection(Pool, Host, Port, Connection),
                    Result;
                {error, _} = Error ->
                    Error
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

%% Internal

-spec get_pool(pool_id(), timeout_t()) -> {ok, pool()} | {error, timeout | _Reason}.
get_pool(PoolID, Timeout) ->
    case get_remaining(Timeout) of
        Remaining when is_integer(Remaining) ->
            gunner_manager:get_pool(PoolID, Remaining);
        timeout ->
            {error, timeout}
    end.

-spec acquire_connection(pool(), conn_host(), conn_port(), timeout_t()) ->
    {ok, connection()} | {error, timeout | _Reason}.
acquire_connection(Pool, Host, Port, Timeout) ->
    case get_remaining(Timeout) of
        Remaining when is_integer(Remaining) ->
            gunner_pool:acquire_connection(Pool, Host, Port, Remaining);
        timeout ->
            {error, timeout}
    end.

-spec do_request(connection(), req_path(), req_method(), req_headers(), body(), req_opts(), timeout_t()) ->
    {ok, response()} | {error, timeout | _Reason}.
do_request(Connection, Path, Method, ReqHeaders, Body, ReqOpts, Timeout) ->
    case get_remaining(Timeout) of
        Remaining when is_integer(Remaining) ->
            gunner_connection:request(Connection, Path, Method, ReqHeaders, Body, ReqOpts, Remaining);
        timeout ->
            {error, timeout}
    end.

%%

-spec init_timeout(TotalMS :: integer()) -> timeout_t().
init_timeout(TotalMS) ->
    Now = erlang:timestamp(),
    {Now, TotalMS}.

-spec calculate_next_timeout(timeout_t()) -> timeout_t() | timeout.
calculate_next_timeout({PreviousTS, PrevousRemaining}) ->
    Now = erlang:timestamp(),
    Diff = timer:now_diff(Now, PreviousTS) div 1000,
    case PrevousRemaining - Diff of
        NewRemaining when NewRemaining > 0 ->
            {Now, NewRemaining};
        _ ->
            timeout
    end.

-spec get_remaining(timeout_t() | timeout) -> Remaining :: integer() | timeout.
get_remaining(timeout) ->
    timeout;
get_remaining({_, Remaining}) ->
    Remaining.

-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

-spec timeout_test() -> _.

timeout_test() ->
    Timeout1 = init_timeout(3000),
    ?assertEqual(3000, get_remaining(Timeout1)),
    _ = timer:sleep(1000),
    Timeout2 = calculate_next_timeout(Timeout1),
    ?assertMatch(NextTime when NextTime > 1900, get_remaining(Timeout2)),
    ?assertMatch(NextTime when NextTime < 2000, get_remaining(Timeout2)),
    _ = timer:sleep(1000),
    Timeout3 = calculate_next_timeout(Timeout2),
    ?assertMatch(NextTime when NextTime > 900, get_remaining(Timeout3)),
    ?assertMatch(NextTime when NextTime < 1000, get_remaining(Timeout3)),
    _ = timer:sleep(1000),
    Timeout4 = calculate_next_timeout(Timeout3),
    ?assertEqual(timeout, get_remaining(Timeout4)).

-endif.
