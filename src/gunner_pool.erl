-module(gunner_pool).

%% API functions

-export([start_pool/2]).
-export([stop_pool/1]).
-export([start_link/2]).

-export([pool_status/2]).

-export([acquire/3]).
-export([free/3]).

%% Gen Server callbacks

-behaviour(gen_server).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% API Types

-type connection_pid() :: pid().

-type pool() :: pid().
-type pool_id() :: atom().
-type pool_mode() :: loose | locking.
-type pool_opts() :: #{
    mode => pool_mode(),
    cleanup_interval => timeout(),
    max_connection_load => size(),
    max_connection_idle_age => size(),
    max_size => size(),
    min_size => size()
}.

-type group_id() :: term().

-type pool_status_response() :: #{total_connections := size(), available_connections := size()}.

-export_type([connection_pid/0]).

-export_type([pool/0]).
-export_type([pool_id/0]).
-export_type([pool_mode/0]).
-export_type([pool_opts/0]).
-export_type([group_id/0]).
-export_type([pool_status_response/0]).

%% Internal types

-record(state, {
    mode = loose :: pool_mode(),
    connections = #{} :: connections(),
    clients = #{} :: clients(),
    counters_ref :: counters:counters_ref(),
    idx_authority :: gunner_idx_authority:t(),
    max_size :: size(),
    min_size :: size(),
    current_size :: size(),
    max_connection_load :: size(),
    max_connection_idle_age :: size(),
    cleanup_interval :: timeout()
}).

-type state() :: #state{}.

-type connections() :: #{connection_pid() => connection_state()}.

-record(connection_state, {
    status = down :: connection_status(),
    group_id :: group_id(),
    idx :: connection_idx(),
    pid :: connection_pid(),
    mref :: mref(),
    idle_count = 0 :: integer()
}).

-type connection_state() :: #connection_state{}.

-type connection_status() :: down | {starting, requester()} | {up, unlocked | {locked, Owner :: pid()}} | inactive.
-type connection_idx() :: gunner_idx_authority:idx().

-type connection_args() :: gunner:connection_args().

-type client_pid() :: pid().
-type clients() :: #{client_pid() => client_state()}.

-record(client_state, {
    connections = [] :: [connection_pid()],
    mref :: mref()
}).

-type client_state() :: #client_state{}.

-type size() :: non_neg_integer().
-type requester() :: from().
-type mref() :: reference().

-type from() :: {pid(), Tag :: _}.

%%

-define(DEFAULT_CLIENT_OPTS, #{connect_timeout => 5000}).

-define(DEFAULT_MODE, loose).

-define(DEFAULT_MAX_CONNECTION_LOAD, 1).
-define(DEFAULT_MAX_CONNECTION_IDLE_AGE, 5).

-define(DEFAULT_MIN_SIZE, 5).
-define(DEFAULT_MAX_SIZE, 25).

-define(DEFAULT_CLEANUP_INTERVAL, 1000).

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
    {ok, connection_pid()} |
    {error, pool_not_found | pool_unavailable | {failed_to_start_connection | connection_failed, _}}.
acquire(PoolID, ConnectionArgs, Timeout) ->
    call_pool(PoolID, {acquire, ConnectionArgs}, Timeout).

-spec free(pool_id(), connection_pid(), timeout()) ->
    ok |
    {error, {invalid_pool_mode, loose} | invalid_connection_state | connection_not_found}.
free(PoolID, ConnectionPid, Timeout) ->
    call_pool(PoolID, {free, ConnectionPid}, Timeout).

-spec pool_status(pool_id(), timeout()) -> {ok, pool_status_response()} | {error, pool_not_found}.
pool_status(PoolID, Timeout) ->
    call_pool(PoolID, pool_status, Timeout).

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
    ({free, connection_pid()}, from(), state()) ->
        {reply, ok | {error, {invalid_pool_mode, loose} | invalid_connection_state}, state()};
    (status, from(), state()) -> {reply, {ok, pool_status_response()}, state()}.
%%(Any :: _, from(), state()) -> no_return().
handle_call({acquire, ConnectionArgs}, From, State) ->
    case handle_acquire_connection(ConnectionArgs, From, State) of
        {{ok, {connection, Connection}}, NewState} ->
            {reply, {ok, Connection}, NewState};
        {{ok, connection_started}, NewState} ->
            {noreply, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({free, ConnectionPid}, From, State) ->
    case handle_free_connection(ConnectionPid, From, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(pool_status, _From, State) ->
    {reply, {ok, create_pool_status_response(State)}, State};
handle_call(_Call, _From, _St) ->
    erlang:error(unexpected_call).

-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast(_Cast, St) ->
    {noreply, St}.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info(?GUNNER_CLEANUP(), State) ->
    {ok, State1} = handle_cleanup(State),
    _ = erlang:send_after(State#state.cleanup_interval, self(), ?GUNNER_CLEANUP()),
    {noreply, State1};
handle_info(?GUN_UP(ConnectionPid), State) ->
    {ok, State1} = handle_connection_started(ConnectionPid, State),
    {noreply, State1};
handle_info(?GUN_DOWN(_ConnectionPid, _Reason), State) ->
    {noreply, State};
handle_info(?DOWN(Mref, ProcessPid, Reason), State) ->
    {ok, State1} = handle_process_down(ProcessPid, Mref, Reason, State),
    {noreply, State1};
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
        mode = maps:get(mode, Opts, ?DEFAULT_MODE),
        max_size = MaxSize,
        min_size = maps:get(min_size, Opts, ?DEFAULT_MIN_SIZE),
        current_size = 0,
        max_connection_load = maps:get(max_connection_load, Opts, ?DEFAULT_MAX_CONNECTION_LOAD),
        idx_authority = gunner_idx_authority:new(MaxSize),
        counters_ref = counters:new(MaxSize, [atomics]),
        connections = #{},
        cleanup_interval = maps:get(cleanup_interval, Opts, ?DEFAULT_CLEANUP_INTERVAL),
        max_connection_idle_age = maps:get(max_connection_idle_age, Opts, ?DEFAULT_MAX_CONNECTION_IDLE_AGE)
    }.

%%

-spec handle_acquire_connection(connection_args(), requester(), state()) -> {Result, state()} | Error when
    Result :: {ok, {connection, connection_pid()} | connection_started},
    Error :: {error, {failed_to_start_connection, _Reason} | pool_unavailable}.
handle_acquire_connection(ConnectionArgs, {ClientPid, _} = From, State) ->
    GroupID = create_group_id(ConnectionArgs),
    case acquire_connection_from_group(GroupID, State) of
        {connection, ConnPid, St1} ->
            {{ok, {connection, ConnPid}}, maybe_lock_connection(ConnPid, ClientPid, St1)};
        no_connection ->
            case handle_connection_creation(GroupID, ConnectionArgs, From, State) of
                {ok, State1} ->
                    {{ok, connection_started}, State1};
                {error, _Reason} = Error ->
                    Error
            end
    end.

-spec create_group_id(connection_args()) -> group_id().
create_group_id(ConnectionArgs) ->
    ConnectionArgs.

-spec acquire_connection_from_group(group_id(), state()) -> {connection, connection_pid(), state()} | no_connection.
acquire_connection_from_group(GroupID, State) ->
    %ConnectionPids = [X || {_, X} <- lists:sort([{rand:uniform(), N} || N <- maps:keys(State#state.connections)])],
    ConnectionPids = maps:keys(State#state.connections),
    case find_suitable_connection(GroupID, ConnectionPids, State) of
        {ok, ConnectionPid} ->
            {ok, ConnectionSt} = get_connection_state(ConnectionPid, State),
            ConnectionSt1 = ConnectionSt#connection_state{idle_count = 0},
            {connection, ConnectionPid, set_connection_state(ConnectionPid, ConnectionSt1, State)};
        {error, no_connection} ->
            no_connection
    end.

-spec find_suitable_connection(group_id(), [connection_pid()], state()) ->
    {ok, connection_pid()} | {error, no_connection}.
find_suitable_connection(_GroupID, [], _State) ->
    {error, no_connection};
find_suitable_connection(GroupID, [Pid | Rest], State) ->
    case get_connection_state(Pid, State) of
        {ok, #connection_state{status = {up, unlocked}, group_id = GroupID, idx = Idx}} ->
            case is_connection_available(Idx, State) of
                true ->
                    {ok, Pid};
                false ->
                    find_suitable_connection(GroupID, Rest, State)
            end;
        _ ->
            find_suitable_connection(GroupID, Rest, State)
    end.

-spec is_connection_available(connection_idx(), state()) -> boolean().
is_connection_available(Idx, State) ->
    ConnectionLoad = get_connection_load(Idx, State),
    ConnectionLoad < State#state.max_connection_load.

-spec get_connection_load(connection_idx(), state()) -> size().
get_connection_load(Idx, State) ->
    counters:get(State#state.counters_ref, Idx).

%%

-spec handle_free_connection(connection_pid(), requester(), state()) -> {Result, state()} | Error when
    Result :: ok,
    Error :: {error, {invalid_pool_mode, loose} | invalid_connection_state | connection_not_found}.
handle_free_connection(_ConnectionPid, _From, #state{mode = loose}) ->
    {error, {invalid_pool_mode, loose}};
handle_free_connection(ConnectionPid, {ClientPid, _} = _From, State = #state{mode = locking}) ->
    case get_connection_state(ConnectionPid, State) of
        {ok, #connection_state{status = {up, {locked, ClientPid}}} = ConnSt} ->
            ConnSt1 = ConnSt#connection_state{idle_count = 0},
            State1 = set_connection_state(ConnectionPid, ConnSt1, State),
            {ok, unlock_connection(ConnectionPid, ClientPid, State1)};
        {ok, #connection_state{}} ->
            {error, invalid_connection_state};
        {error, no_state} ->
            {error, connection_not_found}
    end.

%%

-spec get_connection_state(connection_pid(), state()) -> {ok, connection_state()} | {error, no_state}.
get_connection_state(Pid, State) ->
    case maps:get(Pid, State#state.connections, undefined) of
        #connection_state{} = ConnState ->
            {ok, ConnState};
        undefined ->
            {error, no_state}
    end.

-spec set_connection_state(connection_pid(), connection_state(), state()) -> state().
set_connection_state(Pid, Connection, State) ->
    State#state{connections = maps:put(Pid, Connection, State#state.connections)}.

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
    {ok, NewAthState} = gunner_idx_authority:free_index(Idx, State#state.idx_authority),
    ok = counters:put(State#state.counters_ref, Idx, 0),
    State#state{idx_authority = NewAthState}.

%%

-spec handle_connection_creation(group_id(), connection_args(), requester(), state()) ->
    {ok, state()} | {error, pool_unavailable | {failed_to_start_connection, Reason :: _}}.
handle_connection_creation(GroupID, ConnectionArgs, Requester, State) ->
    case is_pool_available(State) of
        true ->
            {Idx, State1} = new_connection_idx(State),
            case open_gun_connection(ConnectionArgs, Idx, State) of
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

-spec new_connection_state(requester(), group_id(), connection_idx(), connection_pid(), mref()) -> connection_state().
new_connection_state(Requester, GroupID, Idx, Pid, Mref) ->
    #connection_state{
        status = {starting, Requester},
        group_id = GroupID,
        idx = Idx,
        pid = Pid,
        mref = Mref
    }.

%%

-spec open_gun_connection(connection_args(), connection_idx(), state()) ->
    {ok, connection_pid(), mref()} | {error, Reason :: _}.
open_gun_connection({Host, Port}, Idx, State) ->
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

-spec close_gun_connection(connection_pid()) -> ok.
close_gun_connection(ConnectionPid) ->
    ok = gun:close(ConnectionPid).

%%

-spec handle_connection_started(connection_pid(), state()) -> {ok, state()} | {error, invalid_connection_state}.
handle_connection_started(ConnectionPid, State) ->
    case get_connection_state(ConnectionPid, State) of
        {ok, #connection_state{status = {starting, {ClientPid, _} = Requester}} = ConnSt} ->
            ConnSt1 = ConnSt#connection_state{status = {up, unlocked}, idle_count = 0},
            ok = reply_to_requester({ok, ConnectionPid}, Requester),
            State1 = set_connection_state(ConnectionPid, ConnSt1, State),
            {ok, maybe_lock_connection(ConnectionPid, ClientPid, State1)};
        _ ->
            {error, invalid_connection_state}
    end.

reply_to_requester(Message, Requester) ->
    _ = gen_server:reply(Requester, Message),
    ok.

%%

-spec handle_process_down(pid(), mref(), Reason :: _, state()) -> {ok, state()}.
handle_process_down(ProcessPid, Mref, Reason, State) ->
    case is_connection_process(ProcessPid, State) of
        true ->
            handle_connection_down(ProcessPid, Mref, Reason, State);
        false ->
            handle_client_down(ProcessPid, Mref, Reason, State)
    end.

-spec is_connection_process(pid(), state()) -> boolean().
is_connection_process(ProcessPid, State) ->
    maps:is_key(ProcessPid, State#state.connections).

-spec handle_connection_down(connection_pid(), mref(), Reason :: _, state()) ->
    {ok, state()} | {error, invalid_connection_state}.
handle_connection_down(ConnectionPid, Mref, Reason, State) ->
    case get_connection_state(ConnectionPid, State) of
        {ok, #connection_state{status = {starting, Requester}, idx = Idx, mref = Mref}} ->
            ok = reply_to_requester({error, {connection_failed, map_down_reason(Reason)}}, Requester),
            State1 = free_connection_idx(Idx, State),
            {ok, remove_connection_state(ConnectionPid, dec_pool_size(State1))};
        {ok, #connection_state{status = {up, unlocked}, idx = Idx, mref = Mref}} ->
            State1 = free_connection_idx(Idx, State),
            {ok, remove_connection_state(ConnectionPid, dec_pool_size(State1))};
        {ok, #connection_state{status = {up, {locked, ClientPid}}, idx = Idx, mref = Mref}} ->
            State1 = remove_connection_from_client_state(ConnectionPid, ClientPid, State),
            State2 = free_connection_idx(Idx, State1),
            {ok, remove_connection_state(ConnectionPid, dec_pool_size(State2))};
        {ok, #connection_state{status = inactive, mref = Mref}} ->
            {ok, remove_connection_state(ConnectionPid, State)};
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

-spec handle_client_down(client_pid(), mref(), Reason :: _, state()) -> {ok, state()} | {error, invalid_client_state}.
handle_client_down(ClientPid, Mref, _Reason, State) ->
    case get_client_state(ClientPid, State) of
        {ok, #client_state{mref = Mref, connections = Connections}} ->
            {ok, unlock_client_connections(Connections, State)};
        _ ->
            {error, invalid_client_state}
    end.

-spec unlock_client_connections([connection_pid()], state()) -> state().
unlock_client_connections([], State) ->
    State;
unlock_client_connections([ConnectionPid | Rest], State) ->
    State1 = do_unlock_connection(ConnectionPid, State),
    unlock_client_connections(Rest, State1).

%%

-spec handle_cleanup(state()) -> {ok, state()}.
handle_cleanup(State) ->
    case State#state.current_size - State#state.min_size of
        SizeBudget when SizeBudget > 0 ->
            ConnectionPids = maps:keys(State#state.connections),
            cleanup_connections(ConnectionPids, SizeBudget, State);
        _SizeBudget ->
            {ok, State}
    end.

-spec cleanup_connections([connection_pid()], integer(), state()) -> {ok, state()}.
cleanup_connections([], _Budget, State) ->
    {ok, State};
cleanup_connections([Pid | Rest], SizeBudget, State) ->
    {SizeBudget1, State1} = process_connection_cleanup(Pid, SizeBudget, State),
    cleanup_connections(Rest, SizeBudget1, State1).

-spec process_connection_cleanup(connection_pid(), integer(), state()) -> {integer(), state()}.
process_connection_cleanup(Pid, SizeBudget, State) ->
    {ok, ConnSt} = get_connection_state(Pid, State),
    MaxAge = State#state.max_connection_idle_age,
    ConnLoad = get_connection_load(ConnSt#connection_state.idx, State),
    case ConnSt of
        #connection_state{status = {up, _}, idle_count = IdleCount} when IdleCount < MaxAge, ConnLoad =:= 0 ->
            NewConnectionSt = ConnSt#connection_state{idle_count = IdleCount + 1},
            {SizeBudget, set_connection_state(Pid, NewConnectionSt, State)};
        #connection_state{status = {up, _}, idle_count = IdleCount, idx = Idx} when
            IdleCount >= MaxAge, ConnLoad =:= 0, SizeBudget > 0
        ->
            NewConnectionSt = ConnSt#connection_state{status = inactive},
            State1 = free_connection_idx(Idx, State),
            {SizeBudget - 1, set_connection_state(Pid, NewConnectionSt, dec_pool_size(State1))};
        #connection_state{status = {up, _}, idle_count = IdleCount} when
            IdleCount >= MaxAge, ConnLoad =:= 0, SizeBudget =< 0
        ->
            NewConnectionSt = ConnSt#connection_state{idle_count = 0},
            {SizeBudget, set_connection_state(Pid, NewConnectionSt, State)};
        #connection_state{status = {up, _}} when ConnLoad > 0 ->
            NewConnectionSt = ConnSt#connection_state{idle_count = 0},
            {SizeBudget, set_connection_state(Pid, NewConnectionSt, State)};
        #connection_state{status = inactive} ->
            %% 'DOWN' will do everything else
            ok = close_gun_connection(Pid),
            {SizeBudget, State};
        #connection_state{} ->
            {SizeBudget, State}
    end.

%%

-spec maybe_lock_connection(connection_pid(), client_pid(), state()) -> state().
maybe_lock_connection(ConnectionPid, ClientPid, State = #state{mode = locking}) ->
    lock_connection(ConnectionPid, ClientPid, State);
maybe_lock_connection(_ConnectionPid, _ClientPid, State = #state{mode = loose}) ->
    State.

-spec lock_connection(connection_pid(), client_pid(), state()) -> state().
lock_connection(ConnectionPid, ClientPid, State) ->
    State1 = do_lock_connection(ConnectionPid, ClientPid, State),
    ClientSt = get_or_create_client_state(ClientPid, State1),
    ClientSt1 = do_add_connection_to_client(ConnectionPid, ClientSt),
    set_client_state(ClientPid, ClientSt1, State1).

-spec do_lock_connection(connection_pid(), client_pid(), state()) -> state().
do_lock_connection(ConnectionPid, ClientPid, State) ->
    {ok, ConnectionSt} = get_connection_state(ConnectionPid, State),
    ConnectionSt1 = ConnectionSt#connection_state{status = {up, {locked, ClientPid}}},
    set_connection_state(ConnectionPid, ConnectionSt1, State).

-spec do_add_connection_to_client(connection_pid(), client_state()) -> client_state().
do_add_connection_to_client(ConnectionPid, ClientSt = #client_state{connections = Connections}) ->
    ClientSt#client_state{connections = [ConnectionPid | Connections]}.

-spec unlock_connection(connection_pid(), client_pid(), state()) -> state().
unlock_connection(ConnectionPid, ClientPid, State) ->
    State1 = do_unlock_connection(ConnectionPid, State),
    remove_connection_from_client_state(ConnectionPid, ClientPid, State1).

-spec do_unlock_connection(connection_pid(), state()) -> state().
do_unlock_connection(ConnectionPid, State) ->
    {ok, ConnectionSt} = get_connection_state(ConnectionPid, State),
    ConnectionSt1 = ConnectionSt#connection_state{status = {up, unlocked}},
    set_connection_state(ConnectionPid, ConnectionSt1, State).

-spec remove_connection_from_client_state(connection_pid(), client_pid(), state()) -> state().
remove_connection_from_client_state(ConnectionPid, ClientPid, State) ->
    {ok, ClientSt} = get_client_state(ClientPid, State),
    ClientSt1 = do_remove_connection_from_client(ConnectionPid, ClientSt),
    case ClientSt1#client_state.connections of
        [] ->
            true = destroy_client_state(ClientSt1),
            remove_client_state(ClientPid, State);
        _ ->
            set_client_state(ClientPid, ClientSt1, State)
    end.

-spec do_remove_connection_from_client(connection_pid(), client_state()) -> client_state().
do_remove_connection_from_client(ConnectionPid, ClientSt = #client_state{connections = Connections}) ->
    ClientSt#client_state{connections = lists:delete(ConnectionPid, Connections)}.

%%

-spec get_or_create_client_state(client_pid(), state()) -> client_state().
get_or_create_client_state(ClientPid, State) ->
    case get_client_state(ClientPid, State) of
        {ok, ClientSt} ->
            ClientSt;
        {error, no_state} ->
            create_client_state(ClientPid)
    end.

-spec create_client_state(client_pid()) -> client_state().
create_client_state(ClientPid) ->
    Mref = erlang:monitor(process, ClientPid),
    #client_state{
        connections = [],
        mref = Mref
    }.

-spec destroy_client_state(client_state()) -> true.
destroy_client_state(#client_state{mref = Mref}) ->
    true = erlang:demonitor(Mref, [flush]).

-spec get_client_state(client_pid(), state()) -> {ok, client_state()} | {error, no_state}.
get_client_state(Pid, State) ->
    case maps:get(Pid, State#state.clients, undefined) of
        #client_state{} = ClientSt ->
            {ok, ClientSt};
        undefined ->
            {error, no_state}
    end.

-spec set_client_state(client_pid(), client_state(), state()) -> state().
set_client_state(Pid, ClientSt, State) ->
    State#state{clients = maps:put(Pid, ClientSt, State#state.clients)}.

-spec remove_client_state(client_pid(), state()) -> state().
remove_client_state(Pid, State) ->
    State#state{clients = maps:without([Pid], State#state.clients)}.

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

-spec create_pool_status_response(state()) -> pool_status_response().
create_pool_status_response(State) ->
    #{
        total_connections => State#state.current_size,
        available_connections => get_available_connections_count(State#state.connections)
    }.

-spec get_available_connections_count(connections()) -> size().
get_available_connections_count(Connections) ->
    maps:fold(
        fun
            (_K, #connection_state{status = {up, unlocked}}, Acc) -> Acc + 1;
            (_K, #connection_state{}, Acc) -> Acc
        end,
        0,
        Connections
    ).
