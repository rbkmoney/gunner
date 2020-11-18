-module(gunner_manager).

%% API functions

-export([start_link/0]).
-export([get_pool/2]).

%% Gen Server callbacks
-behaviour(gen_server).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).

-define(SERVER, ?MODULE).

%% Internal types
-type state() :: #{
    pools := pools()
}.

-type pool_id() :: gunner_pool:pool_id().
-type pool() :: gunner_pool:pool().

-type pools() :: #{
    pool_id() => pool()
}.

%%
%% API functions
%%

-spec start_link() -> genlib_gen:start_ret().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec get_pool(pool_id(), timeout()) -> {ok, pool()} | {error, _Reason}.
get_pool(PoolID, Timeout) ->
    gen_server:call(?SERVER, {get_pool, PoolID}, Timeout).

%%
%% Gen Server callbacks
%%

-spec init([]) -> {ok, state()}.
init([]) ->
    {ok, new_state()}.

-spec handle_call(any(), _From, state()) -> {reply, {ok, pool()} | {error, _Reason}, state()}.
handle_call({get_pool, PoolID}, _From, St0) ->
    case do_get_pool(PoolID, St0) of
        PoolPID when PoolPID =/= undefined ->
            {reply, {ok, PoolPID}, St0};
        undefined ->
            case create_pool(PoolID) of
                {ok, PoolPID} ->
                    {reply, {ok, PoolPID}, do_set_pool(PoolID, PoolPID, St0)};
                {error, _} = Error ->
                    {reply, Error, St0}
            end
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

-spec do_get_pool(pool_id(), state()) -> pool() | undefined.
do_get_pool(PoolID, #{pools := Pools}) ->
    maps:get(PoolID, Pools, undefined).

-spec do_set_pool(pool_id(), pool(), state()) -> state().
do_set_pool(PoolID, Pool, #{pools := Pools0} = State) ->
    Pools1 = Pools0#{PoolID => Pool},
    State#{pools => Pools1}.

-spec create_pool(pool_id()) -> {ok, pool()} | {error, Reason :: _}.
create_pool(PoolID) ->
    case gunner_pool_sup:start_pool(PoolID) of
        {ok, PoolPID} ->
            _ = erlang:monitor(process, PoolPID),
            {ok, PoolPID};
        {error, _} = Error ->
            Error
    end.

-spec remove_pool_by_pid(pool(), state()) -> state().
remove_pool_by_pid(PoolPID0, #{pools := Pools0} = St0) ->
    Pools1 = maps:filter(
        fun(_, PoolPID) ->
            PoolPID =/= PoolPID0
        end,
        Pools0
    ),
    St0#{pools := Pools1}.
