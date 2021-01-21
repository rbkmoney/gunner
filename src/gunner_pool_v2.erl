-module(gunner_pool_v2).

%% API functions

-export([start_pool/2]).
-export([stop_pool/1]).
-export([start_link/2]).

-export([acquire/3]).

%% Gen Server callbacks

-behaviour(gen_server).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% API Types

-type pool() :: pid().
-type pool_id() :: atom().
-type pool_opts() :: #{
    cleanup_interval => timeout(),
    max_connection_load => size(),
    max_size => size()
}.

-type group_id() :: term().

-type status_response() :: #{total_count := size(), free_count := size()}.

-export_type([pool/0]).
-export_type([pool_id/0]).
-export_type([pool_opts/0]).
-export_type([group_id/0]).
-export_type([status_response/0]).

%% Internal types

-record(state, {
    connections = #{} :: connections(),
    counters_ref :: counters:counters_ref(),
    idx_authority :: gunner_idx_authority:t(),
    max_size :: size(),
    current_size :: size(),
    max_connection_load :: size(),
    cleanup_interval :: timeout()
}).

-type state() :: #state{}.

-type connections() :: #{connection_pid() => connection_state()}.

-record(connection_state, {
    status = down :: connection_status(),
    group_id :: group_id(),
    idx :: connection_idx(),
    pid :: connection_pid(),
    mref :: reference(),
    idle_count = 0 :: integer()
}).

-type connection_state() :: #connection_state{}.

-type connection_status() :: down | {starting, [requester()]} | up | inactive.
-type connection_idx() :: gunner_idx_authority:idx().
-type connection_pid() :: gunner:connection().

-type connection_args() :: gunner:connection_args().

-type size() :: non_neg_integer().
-type requester() :: from().

-type from() :: {pid(), Tag :: _}.

%%

-define(DEFAULT_CLIENT_OPTS, #{connect_timeout => 5000}).

-define(DEFAULT_MAX_CONNECTION_LOAD, 1).
-define(DEFAULT_MAX_SIZE, 25).
-define(DEFAULT_CLEANUP_INTERVAL, 100).

-define(GUN_UP(ConnectionPid), {gun_up, ConnectionPid, _Protocol}).
-define(GUN_DOWN(ConnectionPid, Reason), {gun_down, ConnectionPid, _Protocol, Reason, _KilledStreams}).
-define(DOWN(Mref, ConnectionPid, Reason), {'DOWN', Mref, process, ConnectionPid, Reason}).

-define(GUNNER_CLEANUP(), {gunner_cleanup}).

%%
%% API functions
%%

-spec start_pool(pool_id(), pool_opts()) -> ok | {error, already_exists}.
start_pool(PoolID, PoolOpts) ->
    case gunner_pool_sup:start_pool(PoolID, PoolOpts) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _}} ->
            {error, already_exists}
    end.

-spec stop_pool(pool_id()) -> ok | {error, not_found}.
stop_pool(PoolID) ->
    case get_pool_pid(PoolID) of
        Pid when is_pid(Pid) ->
            gunner_pool_sup:stop_pool(Pid);
        undefined ->
            {error, not_found}
    end.

-spec start_link(pool_id(), pool_opts()) -> genlib_gen:start_ret().
start_link(PoolID, PoolOpts) ->
    gen_server:start_link(via_tuple(PoolID), ?MODULE, [PoolOpts], []).

%%

-spec acquire(pool_id(), connection_args(), timeout()) ->
    {ok, connection_pid()} | {error, pool_not_found | pool_unavailable | {connection_failed, _}}.
acquire(PoolID, ConnectionArgs, Timeout) ->
    call_pool(PoolID, {acquire, ConnectionArgs}, Timeout).

%% API helpers

-spec call_pool(pool_id(), Args :: _, timeout()) -> Response :: _ | no_return().
call_pool(PoolID, Args, Timeout) ->
    try
        gen_server:call(via_tuple(PoolID), Args, Timeout)
    catch
        exit:{noproc, _} ->
            {error, pool_not_found}
    end.

-spec via_tuple(pool_id()) -> gunner_pool_registry:via_tuple().
via_tuple(PoolID) ->
    gunner_pool_registry:via_tuple(PoolID).

-spec get_pool_pid(pool_id()) -> pid() | undefined.
get_pool_pid(PoolID) ->
    gunner_pool_registry:whereis(PoolID).

%%
%% Gen Server callbacks
%%

-spec init(list()) -> {ok, state()}.
init([PoolOpts]) ->
    State = new_state(PoolOpts),
    _ = erlang:send_after(State#state.cleanup_interval, self(), ?GUNNER_CLEANUP()),
    {ok, State}.

-spec handle_call
    ({acquire, connection_args()}, from(), state()) ->
        {noreply, state()} |
        {reply, {ok, connection_pid()} | {error, pool_unavailable | {failed_to_start_connection, Reason :: _}},
            state()};
    (status, from(), state()) -> {reply, {ok, status_response()}, state()}.
%%(Any :: _, from(), state()) -> no_return().
handle_call({acquire, ConnectionArgs}, From, State) ->
    {Result, NewState} = handle_acquire_connection(ConnectionArgs, From, State),
    case Result of
        {ok, {connection, Connection}} ->
            {reply, {ok, Connection}, NewState};
        {ok, connection_started} ->
            {noreply, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(_Call, _From, _St) ->
    erlang:error(unexpected_call).

-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast(_Cast, St) ->
    {noreply, St}.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info(?GUNNER_CLEANUP, St0) ->
    {ok, St1} = handle_cleanup(St0),
    {noreply, St1};
handle_info(?GUN_UP(ConnectionPid), St0) ->
    {ok, St1} = handle_connection_started(ConnectionPid, St0),
    {noreply, St1};
handle_info(?GUN_DOWN(_ConnectionPid, _Reason), St0) ->
    {noreply, St0};
handle_info(?DOWN(Mref, ConnectionPid, Reason), St0) ->
    {ok, St1} = handle_connection_down(ConnectionPid, Mref, Reason, St0),
    {noreply, St1};
handle_info(_, _St0) ->
    erlang:error(unexpected_info).

-spec terminate(any(), state()) -> ok.
terminate(_, _St) ->
    ok.

%%

-spec new_state(pool_opts()) -> state().
new_state(Opts) ->
    MaxSize = maps:get(max_size, Opts, ?DEFAULT_MAX_SIZE),
    #state{
        max_size = MaxSize,
        current_size = 0,
        max_connection_load = maps:get(max_connection_load, Opts, ?DEFAULT_MAX_CONNECTION_LOAD),
        idx_authority = gunner_idx_authority:new(MaxSize),
        counters_ref = counters:new(MaxSize, [atomics]),
        connections = #{},
        cleanup_interval = maps:get(cleanup_interval, Opts, ?DEFAULT_CLEANUP_INTERVAL)
    }.

%%

-spec handle_acquire_connection(connection_args(), requester(), state()) -> {Result, state()} when
    Result ::
        {ok, {connection, connection_pid()} | connection_started} |
        {error, {failed_to_start_connection, _Reason} | pool_unavailable}.
handle_acquire_connection(ConnectionArgs, From, State) ->
    GroupID = create_group_id(ConnectionArgs),
    case acquire_connection_from_group(GroupID, State) of
        {connection, ConnPid, St1} ->
            {{ok, {connection, ConnPid}}, St1};
        no_connection ->
            case handle_connection_creation(GroupID, ConnectionArgs, From, State) of
                {ok, State1} ->
                    {{ok, connection_started}, State1};
                {error, _Reason} = Error ->
                    {Error, State}
            end
    end.

-spec create_group_id(connection_args()) -> group_id().
create_group_id(ConnectionArgs) ->
    ConnectionArgs.

-spec acquire_connection_from_group(group_id(), state()) -> {connection, connection_pid(), state()} | no_connection.
acquire_connection_from_group(GroupID, State) ->
    ConnectionIndices = maps:keys(State#state.connections),
    case find_suitable_connection(GroupID, ConnectionIndices, State) of
        {ok, Idx, Connection} ->
            ConnectionSt = get_connection_state(Idx, State),
            ConnectionSt1 = ConnectionSt#connection_state{idle_count = 0},
            {connection, Connection, set_connection_state(Idx, ConnectionSt1, State)};
        {error, no_connection} ->
            no_connection
    end.

-spec find_suitable_connection(group_id(), [connection_pid()], state()) ->
    {ok, connection_idx(), connection_pid()} | {error, no_connection}.
find_suitable_connection(_GroupID, [], _State) ->
    {error, no_connection};
find_suitable_connection(GroupID, [Pid | Rest], State) ->
    case get_connection_state(Pid, State) of
        Connection = #connection_state{status = up, group_id = GroupID, idx = Idx} ->
            case is_connection_available(Idx, State) of
                true ->
                    {ok, Idx, Pid};
                false ->
                    find_suitable_connection(GroupID, Rest, State)
            end;
        _ ->
            find_suitable_connection(GroupID, Rest, State)
    end.

-spec is_connection_available(connection_idx(), state()) -> boolean().
is_connection_available(Idx, State) ->
    ConnectionLoad = get_connection_load(Idx, State),
    ConnectionLoad >= State#state.max_connection_load.

-spec get_connection_load(connection_idx(), state()) -> size().
get_connection_load(Idx, State) ->
    counters:get(State#state.counters_ref, Idx).

%%

-spec get_connection_state(connection_pid(), state()) -> connection_state().
get_connection_state(Pid, State) ->
    maps:get(Pid, State#state.connections).

-spec set_connection_state(connection_pid(), connection_state(), state()) -> state().
set_connection_state(Pid, Connection, State) ->
    State#state{connections = maps:set(Pid, Connection, State#state.connections)}.

-spec remove_connection_state(connection_pid(), state()) -> state().
remove_connection_state(Pid, State) ->
    State#state{connections = maps:without([Pid], State#state.connections)}.

%%

-spec new_connection_idx(state()) -> {connection_idx(), state()}.
new_connection_idx(State) ->
    {ok, Idx, NewAthState} = gunner_idx_authority:get_index(State#state.idx_authority),
    {Idx, State#state{idx_authority = NewAthState}}.

-spec free_connection_idx(connection_idx(), state()) -> state().
free_connection_idx(Idx, State) ->
    NewAthState = gunner_idx_authority:free_index(Idx, State#state.idx_authority),
    ok = counters:put(State#state.counters_ref, Idx, 0),
    State#state{idx_authority = NewAthState}.

%%
-spec handle_connection_creation(group_id(), connection_args(), requester(), state()) ->
    {ok, state()} | {error, pool_unavailable | {failed_to_start_connection, Reason :: _}}.
handle_connection_creation(GroupID, ConnectionArgs, Requester, State) ->
    case is_pool_available(State) of
        true ->
            {Idx, State1} = new_connection_idx(State),
            case start_new_connection(ConnectionArgs, Idx, State) of
                {ok, Pid, Mref} ->
                    ConnectionState = new_connection_state(Requester, GroupID, Idx, Pid, Mref),
                    {ok, set_connection_state(Pid, ConnectionState, inc_pool_size(State1))};
                {error, Reason} ->
                    {error, {failed_to_start_connection, Reason}}
            end;
        false ->
            {error, pool_unavailable}
    end.

-spec is_pool_available(state()) -> boolean().
is_pool_available(State) ->
    State#state.current_size < get_total_limit(State).

-spec get_total_limit(state()) -> size().
get_total_limit(State) ->
    State#state.max_size.

-spec new_connection_state(requester(), group_id(), connection_idx(), connection_pid(), reference()) ->
    connection_state().
new_connection_state(Requester, GroupID, Idx, Pid, Mref) ->
    #connection_state{
        status = {starting, [Requester]},
        group_id = GroupID,
        idx = Idx,
        pid = Pid,
        mref = Mref
    }.

start_new_connection({Host, Port}, Idx, State) ->
    Opts = get_gun_opts(Idx, State),
    case gun:open(Host, Port, Opts) of
        {ok, Connection} ->
            Mref = erlang:monitor(process, Connection),
            {ok, Connection, Mref};
        {error, _Reason} = Error ->
            Error
    end.

-spec get_gun_opts(connection_idx(), state()) -> gun:opts().
get_gun_opts(Idx, State) ->
    Opts = genlib_app:env(gunner, client_opts, ?DEFAULT_CLIENT_OPTS),
    EventHandler = maps:with([event_handler], Opts),
    Opts#{
        retry => 0,
        %% Setup event handler and optionally pass through a custom one
        event_handler =>
            {gunner_gun_event_handler, EventHandler#{
                counters_ref => State#state.counters_ref,
                counters_idx => Idx,
                streams => #{}
            }}
    }.

%%

-spec handle_connection_started(connection_pid(), state()) -> {ok, state()} | {error, invalid_connection_state}.
handle_connection_started(ConnectionPid, State) ->
    case get_connection_state(ConnectionPid, State) of
        ConnSt = #connection_state{status = {starting, Requesters}} ->
            ConnSt1 = ConnSt#connection_state{status = up, idle_count = 0},
            ok = reply_to_requesters({ok, ConnectionPid}, Requesters),
            {ok, set_connection_state(ConnectionPid, ConnSt1, State)};
        _ ->
            {error, invalid_connection_state}
    end.

reply_to_requesters(_Message, []) ->
    ok;
reply_to_requesters(Message, [Requester | Rest]) ->
    _ = gen_server:reply(Requester, Message),
    reply_to_requesters(Message, Rest).

%%

-spec handle_connection_down(connection_pid(), reference(), Reason :: _, state()) ->
    {ok, state()} | {error, invalid_connection_state}.
handle_connection_down(ConnectionPid, Mref, Reason, State) ->
    case get_connection_state(ConnectionPid, State) of
        #connection_state{status = {starting, Requesters}, idx = Idx, mref = Mref} ->
            ok = reply_to_requesters({error, {connection_failed, map_down_reason(Reason)}}, Requesters),
            State1 = free_connection_idx(Idx, State),
            {ok, remove_connection_state(ConnectionPid, dec_pool_size(State1))};
        #connection_state{status = Status, idx = Idx, mref = Mref} when Status =:= up, Status =:= inactive ->
            State1 = free_connection_idx(Idx, State),
            {ok, remove_connection_state(ConnectionPid, dec_pool_size(State1))};
        _ ->
            {error, invalid_connection_state}
    end.

map_down_reason(noproc) ->
    %% Connection could've died even before we put a monitor on it
    %% Because of this, we have no idea what happened
    %% @TODO Consider using the event handler for "soft" failures
    unknown;
map_down_reason(Reason) ->
    Reason.

%%

handle_cleanup(St0) ->
    


-spec cleanup_connections([connection_pid()], MaxAge :: integer(), state()) ->
    {ok, state()}.
cleanup_connections([],  _MaxAge, State) ->
    {ok, State};
cleanup_connections([Pid | Rest], MaxAge, State) ->
    ConnectionSt = get_connection_state(Pid, State),
    case cleanup_connection(ConnectionSt, MaxAge, State) of
        ConnectionSt ->
            find_suitable_connection(Rest, MaxAge, State);
        NewConnectionSt ->
            find_suitable_connection(Rest, MaxAge, set_connection_state(Pid, NewConnectionSt, State))
    end.

cleanup_connection(St = #connection_state{status = up, idle_count = IdleCount, idx = Idx}, MaxAge, State) when IdleCount < MaxAge ->
    case get_connection_load(Idx, State) of
        0 ->
            ConnectionSt#connection_state{idle_count = IdleCount + 1};
        _ ->
            ConnectionSt;
    end;
cleanup_connection(St = #connection_state{status = up, idle_count = IdleCount, idx = Idx}, MaxAge, State) when IdleCount >= MaxAge ->
    case get_connection_load(Idx, State) of
        0 ->
            ConnectionSt#connection_state{status = inactive};
        _ ->
            ConnectionSt;
    end;
cleanup_connection(St = #connection_state{status = up, idle_count = IdleCount, idx = Idx}, MaxAge, State) when IdleCount >= MaxAge ->
    case get_connection_load(Idx, State) of
        0 ->
            ConnectionSt#connection_state{status = inactive};
        _ ->
            ConnectionSt;
    end;

%%

-spec inc_pool_size(state()) -> state().
inc_pool_size(State) ->
    offset_pool_size(State, 1).

-spec dec_pool_size(state()) -> state().
dec_pool_size(State) ->
    offset_pool_size(State, -1).

-spec offset_pool_size(state(), integer()) -> state().
offset_pool_size(State, Inc) ->
    State#state{current_size = State#state.current_size + Inc}.
