-module(gunner_manager).

%% API functions

-export([start_link/0]).

-export([start_pool/3]).
-export([stop_pool/2]).
-export([get_pool/2]).

%% Gen Server callbacks

-behaviour(gen_server).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).

-define(SERVER, ?MODULE).

%% API Types

-type pool_id() :: term().

-export_type([pool_id/0]).

%% Internal types

-type state() :: #{
    pools := pools()
}.

-type pool_pid() :: gunner_pool:pool().
-type pool_opts() :: gunner_pool:pool_opts().

-type pools() :: #{
    pool_id() => pool_pid()
}.

%%
%% API functions
%%

-spec start_link() -> genlib_gen:start_ret().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec start_pool(pool_id(), pool_opts(), timeout()) -> ok | {error, already_exists}.
start_pool(PoolID, PoolOpts, Timeout) ->
    gen_server:call(?SERVER, {start_pool, PoolID, PoolOpts}, Timeout).

-spec stop_pool(pool_id(), timeout()) -> ok | {error, not_found}.
stop_pool(PoolID, Timeout) ->
    gen_server:call(?SERVER, {stop_pool, PoolID}, Timeout).

-spec get_pool(pool_id(), timeout()) -> {ok, pool_pid()} | {error, not_found}.
get_pool(PoolID, Timeout) ->
    gen_server:call(?SERVER, {get_pool, PoolID}, Timeout).

%%
%% Gen Server callbacks
%%

-spec init([]) -> {ok, state()}.
init([]) ->
    {ok, new_state()}.

-spec handle_call
    ({start_pool, pool_id(), pool_opts()}, _From, state()) -> {reply, ok | {error, already_exists}, state()};
    ({stop_pool, pool_id()}, _From, state()) -> {reply, ok | {error, not_found}, state()};
    ({get_pool, pool_id(), pool_opts()}, _From, state()) -> {reply, {ok, pool_pid()} | {error, not_found}, state()}.
handle_call({start_pool, PoolID, PoolOpts}, _From, St0 = #{pools := Pools0}) ->
    case do_get_pool(PoolID, Pools0) of
        undefined ->
            {ok, PoolPID} = start_pool(PoolOpts),
            Pools1 = do_set_pool(PoolID, PoolPID, Pools0),
            {reply, ok, St0#{pools := Pools1}};
        PoolPID when PoolPID =/= undefined ->
            {reply, {error, already_exists}, St0}
    end;
handle_call({stop_pool, PoolID}, _From, St0 = #{pools := Pools0}) ->
    case do_get_pool(PoolID, Pools0) of
        PoolPID when PoolPID =/= undefined ->
            ok = stop_pool(PoolPID),
            Pools1 = do_remove_pool(PoolID, Pools0),
            {reply, ok, St0#{pools := Pools1}};
        undefined ->
            {reply, {error, not_found}, St0}
    end;
handle_call({get_pool, PoolID}, _From, St0 = #{pools := Pools0}) ->
    case do_get_pool(PoolID, Pools0) of
        PoolPID when PoolPID =/= undefined ->
            {reply, {ok, PoolPID}, St0};
        undefined ->
            {reply, {error, not_found}, St0}
    end;
handle_call(_Call, _From, _St) ->
    erlang:error(unexpected_call).

-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast(_Cast, St) ->
    {noreply, St}.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({'DOWN', _Mref, process, PoolPID, _Reason}, St0) ->
    St1 = remove_pool_by_pid(PoolPID, St0),
    {noreply, St1}.

%%
%% Internal functions
%%

-spec new_state() -> state().
new_state() ->
    #{
        pools => #{}
    }.

-spec do_get_pool(pool_id(), pools()) -> pool_pid() | undefined.
do_get_pool(PoolID, Pools) ->
    maps:get(PoolID, Pools, undefined).

-spec do_set_pool(pool_id(), pool_pid(), pools()) -> pools().
do_set_pool(PoolID, Pool, Pools0) ->
    Pools0#{PoolID => Pool}.

-spec do_remove_pool(pool_id(), pools()) -> pools().
do_remove_pool(PoolID, Pools0) ->
    maps:without([PoolID], Pools0).

-spec start_pool(pool_opts()) -> {ok, pool_pid()} | {error, Reason :: _}.
start_pool(PoolOpts) ->
    case gunner_pool_sup:start_pool(PoolOpts) of
        {ok, PoolPID} ->
            _ = erlang:monitor(process, PoolPID),
            {ok, PoolPID};
        {error, _} = Error ->
            Error
    end.

-spec stop_pool(pool_pid()) -> ok | {error, Reason :: _}.
stop_pool(PoolPID) ->
    gunner_pool_sup:stop_pool(PoolPID).

-spec remove_pool_by_pid(pool_pid(), state()) -> state().
remove_pool_by_pid(PoolPID0, #{pools := Pools0} = St0) ->
    Pools1 = maps:filter(
        fun(_, PoolPID) ->
            PoolPID =/= PoolPID0
        end,
        Pools0
    ),
    St0#{pools := Pools1}.
