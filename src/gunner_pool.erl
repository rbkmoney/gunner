-module(gunner_pool).

%% API functions

-export([start_pool/1]).
-export([start_pool/2]).
-export([stop_pool/1]).
-export([start_link/2]).

-export([pool_status/2]).

-export([acquire/3]).
-export([acquire/4]).
-export([free/2]).

%% Gen Server callbacks

-behaviour(gen_server).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% API Types

-type connection_pid() :: pid().

-type pool_pid() :: pid().
-type pool_reg_name() :: genlib_gen:reg_name().
-type pool_id() :: genlib_gen:ref().

%% @doc Interval at which the cleanup operation in pool is performed, in ms
%% Cleanup is not quaranteed to be excecuted every X ms, instead a minimum of X ms is guaranteed to
%% pass after the previous cleanup is finished
-type cleanup_interval() :: timeout().

%% @doc Maximum amount of opened streams for connection to become unavailable to be acquired, almost always 1.
-type max_connection_load() :: max_connection_load().

%% @doc Maximum amount time of time in ms a connection can remain idle before being deleted.
-type max_connection_idle_age() :: size().

%% @doc Maximum amount of connections in pool. When this value is reached, and a new connection must be opened
%% to satisfy an 'acquire' pool_unavailable will be returned instead.
-type max_size() :: size().

%% @doc Mininum amount of connections kept in pool. This is a soft limit, so connections must be first opened
%% naturally by 'acquire' requests. When pool size is at this value, no more connections are killed by
%% cleanup operations.
-type min_size() :: size().

%% @doc Gun opts used when creating new connections in this pool. See `gun:opts/0`.
-type connection_opts() :: gun:opts().

-type pool_opts() :: #{
    cleanup_interval => cleanup_interval(),
    max_connection_load => max_connection_load(),
    max_connection_idle_age => max_connection_idle_age(),
    max_size => max_size(),
    min_size => min_size(),
    connection_opts => connection_opts()
}.

-type group_id() :: term().

-type pool_status_response() :: #{total_connections := size(), available_connections := size()}.

-type acquire_error() ::
    pool_not_found |
    pool_unavailable |
    {failed_to_start_connection | connection_failed, _}.

-type locking() :: boolean().

-export_type([connection_pid/0]).

-export_type([pool_pid/0]).
-export_type([pool_reg_name/0]).
-export_type([pool_id/0]).

-export_type([locking/0]).

-export_type([cleanup_interval/0]).
-export_type([max_connection_load/0]).
-export_type([max_connection_idle_age/0]).
-export_type([max_size/0]).
-export_type([min_size/0]).
-export_type([pool_opts/0]).

-export_type([group_id/0]).
-export_type([pool_status_response/0]).

-export_type([acquire_error/0]).

%% Internal types

-record(state, {
    connections = #{} :: connections(),
    connection_opts :: connection_opts(),
    clients = #{} :: clients(),
    counters_ref :: counters:counters_ref(),
    idx_authority :: gunner_idx_authority:t(),
    max_size :: max_size(),
    min_size :: min_size(),
    active_count :: size(),
    starting_count :: size(),
    max_connection_load :: max_connection_load(),
    max_connection_idle_age :: max_connection_idle_age(),
    cleanup_interval :: cleanup_interval()
}).

-type state() :: #state{}.

-type connections() :: #{connection_pid() => connection_state()}.

-record(connection_state, {
    status = down :: connection_status(),
    lock = unlocked :: connection_lock(),
    group_id :: group_id(),
    idx :: connection_idx(),
    pid :: connection_pid(),
    idle_since = 0 :: integer()
}).

-type connection_state() :: #connection_state{}.

-type connection_status() :: {starting, requester(), locking()} | up | down.
-type connection_lock() :: unlocked | {locked, Owner :: client_pid()}.
-type connection_idx() :: gunner_idx_authority:idx().

-type endpoint() :: gunner:endpoint().

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

-define(DEFAULT_CONNECTION_OPTS, #{}).

-define(DEFAULT_MAX_CONNECTION_LOAD, 1).
-define(DEFAULT_MAX_CONNECTION_IDLE_AGE, 5).

-define(DEFAULT_MIN_SIZE, 5).
-define(DEFAULT_MAX_SIZE, 25).

-define(DEFAULT_CLEANUP_INTERVAL, 1000).

-define(GUN_UP(ConnectionPid), {gun_up, ConnectionPid, _Protocol}).
-define(GUN_DOWN(ConnectionPid, Reason), {gun_down, ConnectionPid, _Protocol, Reason, _KilledStreams}).
-define(DOWN(Mref, Pid, Reason), {'DOWN', Mref, process, Pid, Reason}).
-define(EXIT(Pid, Reason), {'EXIT', Pid, Reason}).

-define(GUNNER_CLEANUP(), {gunner_cleanup}).

%%
%% API functions
%%

-spec start_pool(pool_opts()) -> {ok, pid()}.
start_pool(PoolOpts) ->
    gunner_pool_sup:start_pool(undefined, PoolOpts).

-spec start_pool(pool_reg_name() | undefined, pool_opts()) -> {ok, pid()} | {error, already_exists}.
start_pool(PoolRegName, PoolOpts) ->
    case gunner_pool_sup:start_pool(PoolRegName, PoolOpts) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, _}} ->
            {error, already_exists}
    end.

-spec stop_pool(pid()) -> ok | {error, pool_not_found}.
stop_pool(Pid) ->
    case gunner_pool_sup:stop_pool(Pid) of
        ok ->
            ok;
        {error, not_found} ->
            {error, pool_not_found}
    end.

-spec start_link(pool_reg_name() | undefined, pool_opts()) -> genlib_gen:start_ret().
start_link(undefined, PoolOpts) ->
    gen_server:start_link(?MODULE, [PoolOpts], []);
start_link(PoolRegName, PoolOpts) ->
    gen_server:start_link(PoolRegName, ?MODULE, [PoolOpts], []).

%%

-spec acquire(pool_id(), endpoint(), timeout()) ->
    {ok, connection_pid()} |
    {error, acquire_error()}.
acquire(PoolID, Endpoint, Timeout) ->
    call_pool(PoolID, {acquire, Endpoint, false}, Timeout).

-spec acquire(pool_id(), endpoint(), locking(), timeout()) ->
    {ok, connection_pid()} |
    {error, acquire_error()}.
acquire(PoolID, Endpoint, Locking, Timeout) ->
    call_pool(PoolID, {acquire, Endpoint, Locking}, Timeout).

-spec free(pool_id(), connection_pid()) -> ok.
free(PoolID, ConnectionPid) ->
    gen_server:cast(PoolID, {free, ConnectionPid, self()}).

-spec pool_status(pool_id(), timeout()) -> {ok, pool_status_response()} | {error, pool_not_found}.
pool_status(PoolID, Timeout) ->
    call_pool(PoolID, pool_status, Timeout).

%% API helpers

-spec call_pool(pool_id(), Args :: _, timeout()) -> Response :: _ | no_return().
call_pool(PoolRef, Args, Timeout) ->
    try
        gen_server:call(PoolRef, Args, Timeout)
    catch
        exit:{noproc, _} ->
            {error, pool_not_found}
    end.

%%
%% Gen Server callbacks
%%

-spec init(list()) -> {ok, state()}.
init([PoolOpts]) ->
    State = new_state(PoolOpts),
    _ = erlang:process_flag(trap_exit, true),
    _ = erlang:send_after(State#state.cleanup_interval, self(), ?GUNNER_CLEANUP()),
    {ok, State}.

-spec handle_call
    ({acquire, endpoint(), locking()}, from(), state()) ->
        {noreply, state()} |
        {reply, {ok, connection_pid()} | {error, pool_unavailable | {failed_to_start_connection, Reason :: _}},
            state()};
    (status, from(), state()) -> {reply, {ok, pool_status_response()}, state()}.
handle_call({acquire, Endpoint, Locking}, From, State) ->
    case handle_acquire_connection(Endpoint, Locking, From, State) of
        {{ok, {connection, Connection}}, NewState} ->
            {reply, {ok, Connection}, NewState};
        {{ok, connection_started}, NewState} ->
            {noreply, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(pool_status, _From, State) ->
    {reply, {ok, create_pool_status_response(State)}, State}.

-spec handle_cast({free, connection_pid(), client_pid()}, state()) -> {noreply, state()}.
handle_cast({free, ConnectionPid, ClientPid}, State) ->
    {noreply, handle_free_connection(ConnectionPid, ClientPid, State)}.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info(?GUNNER_CLEANUP(), State) ->
    State1 = handle_cleanup(State),
    _ = erlang:send_after(State#state.cleanup_interval, self(), ?GUNNER_CLEANUP()),
    {noreply, State1};
handle_info(?GUN_UP(ConnectionPid), State) ->
    State1 = handle_connection_started(ConnectionPid, State),
    {noreply, State1};
handle_info(?GUN_DOWN(_ConnectionPid, _Reason), State) ->
    {noreply, State};
handle_info(?DOWN(Mref, ClientPid, Reason), State) ->
    State1 = handle_client_down(ClientPid, Mref, Reason, State),
    {noreply, State1};
handle_info(?EXIT(ConnectionPid, Reason), State) ->
    State1 = handle_connection_down(ConnectionPid, Reason, State),
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
        max_size = MaxSize,
        min_size = maps:get(min_size, Opts, ?DEFAULT_MIN_SIZE),
        active_count = 0,
        starting_count = 0,
        max_connection_load = maps:get(max_connection_load, Opts, ?DEFAULT_MAX_CONNECTION_LOAD),
        idx_authority = gunner_idx_authority:new(MaxSize),
        counters_ref = counters:new(MaxSize, [atomics]),
        connections = #{},
        connection_opts = maps:get(connection_opts, Opts, ?DEFAULT_CONNECTION_OPTS),
        cleanup_interval = maps:get(cleanup_interval, Opts, ?DEFAULT_CLEANUP_INTERVAL),
        max_connection_idle_age = maps:get(max_connection_idle_age, Opts, ?DEFAULT_MAX_CONNECTION_IDLE_AGE)
    }.

%%

-spec handle_acquire_connection(endpoint(), locking(), requester(), state()) -> {Result, state()} | Error when
    Result :: {ok, {connection, connection_pid()} | connection_started},
    Error :: {error, {failed_to_start_connection, _Reason} | pool_unavailable}.
handle_acquire_connection(Endpoint, Locking, {ClientPid, _} = From, State) ->
    GroupID = create_group_id(Endpoint),
    case acquire_connection_from_group(GroupID, State) of
        {connection, ConnPid, St1} ->
            {{ok, {connection, ConnPid}}, maybe_lock_connection(Locking, ConnPid, ClientPid, St1)};
        no_connection ->
            case handle_connection_creation(GroupID, Endpoint, From, Locking, State) of
                {ok, State1} ->
                    {{ok, connection_started}, State1};
                {error, _Reason} = Error ->
                    Error
            end
    end.

-spec create_group_id(endpoint()) -> group_id().
create_group_id(Endpoint) ->
    Endpoint.

-spec acquire_connection_from_group(group_id(), state()) -> {connection, connection_pid(), state()} | no_connection.
acquire_connection_from_group(GroupID, State) ->
    case find_suitable_connection(GroupID, State) of
        {ok, ConnState} ->
            ConnPid = ConnState#connection_state.pid,
            ConnState1 = reset_connection_idle(ConnState),
            {connection, ConnPid, set_connection_state(ConnPid, ConnState1, State)};
        {error, no_connection} ->
            no_connection
    end.

-spec find_suitable_connection(group_id(), state()) -> {ok, connection_state()} | {error, no_connection}.
find_suitable_connection(GroupID, State) ->
    % @TODO Randomize iteration order somehow?
    Iter = maps:iterator(State#state.connections),
    find_suitable_connection(maps:next(Iter), GroupID, State).

-type conn_it() :: maps:iterator(connection_pid(), connection_state()).
-type next_conn() :: {connection_pid(), connection_state(), conn_it()} | none.

-spec find_suitable_connection(next_conn(), group_id(), state()) -> {ok, connection_state()} | {error, no_connection}.
find_suitable_connection(none, _GroupID, _State) ->
    {error, no_connection};
find_suitable_connection({_ConnPid, ConnState, Iter}, GroupID, State) ->
    case ConnState of
        #connection_state{status = up, lock = unlocked, group_id = GroupID, idx = Idx} ->
            case is_connection_available(Idx, State) of
                true ->
                    {ok, ConnState};
                false ->
                    find_suitable_connection(maps:next(Iter), GroupID, State)
            end;
        _ ->
            find_suitable_connection(maps:next(Iter), GroupID, State)
    end.

-spec is_connection_available(connection_idx(), state()) -> boolean().
is_connection_available(Idx, State) ->
    ConnectionLoad = get_connection_load(Idx, State),
    ConnectionLoad < State#state.max_connection_load.

-spec get_connection_load(connection_idx(), state()) -> size().
get_connection_load(Idx, State) ->
    counters:get(State#state.counters_ref, Idx).

%%

-spec handle_free_connection(connection_pid(), client_pid(), state()) -> state().
handle_free_connection(ConnectionPid, ClientPid, State) ->
    case get_connection_state(ConnectionPid, State) of
        #connection_state{status = up, lock = {locked, ClientPid}} = ConnSt ->
            ConnSt1 = reset_connection_idle(ConnSt),
            State1 = set_connection_state(ConnectionPid, ConnSt1, State),
            unlock_connection(ConnectionPid, ClientPid, State1);
        _ ->
            State
    end.

%%

-spec get_connection_state(connection_pid(), state()) -> connection_state() | undefined.
get_connection_state(Pid, State) ->
    maps:get(Pid, State#state.connections, undefined).

-spec set_connection_state(connection_pid(), connection_state(), state()) -> state().
set_connection_state(Pid, Connection, State) ->
    State#state{connections = maps:put(Pid, Connection, State#state.connections)}.

-spec remove_connection_state(connection_pid(), state()) -> state().
remove_connection_state(Pid, State) ->
    State#state{connections = maps:remove(Pid, State#state.connections)}.

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

-spec handle_connection_creation(group_id(), endpoint(), requester(), locking(), state()) ->
    {ok, state()} | {error, pool_unavailable | {failed_to_start_connection, Reason :: _}}.
handle_connection_creation(GroupID, Endpoint, Requester, Locking, State) ->
    case is_pool_available(State) of
        true ->
            {Idx, State1} = new_connection_idx(State),
            case open_gun_connection(Endpoint, Idx, State) of
                {ok, Pid} ->
                    ConnectionState = new_connection_state(Requester, GroupID, Idx, Pid, Locking),
                    {ok, set_connection_state(Pid, ConnectionState, inc_starting_count(State1))};
                {error, Reason} ->
                    {error, {failed_to_start_connection, Reason}}
            end;
        false ->
            {error, pool_unavailable}
    end.

-spec is_pool_available(state()) -> boolean().
is_pool_available(State) ->
    (State#state.active_count + State#state.starting_count) < get_total_limit(State).

-spec get_total_limit(state()) -> max_size().
get_total_limit(State) ->
    State#state.max_size.

-spec new_connection_state(requester(), group_id(), connection_idx(), connection_pid(), locking()) ->
    connection_state().
new_connection_state(Requester, GroupID, Idx, Pid, Locking) ->
    #connection_state{
        status = {starting, Requester, Locking},
        lock = unlocked,
        group_id = GroupID,
        idx = Idx,
        pid = Pid
    }.

%%

-spec open_gun_connection(endpoint(), connection_idx(), state()) -> {ok, connection_pid()} | {error, Reason :: _}.
open_gun_connection({Host, Port}, Idx, State) ->
    Opts = get_gun_opts(Idx, State),
    gun:open(Host, Port, Opts).

-spec get_gun_opts(connection_idx(), state()) -> gun:opts().
get_gun_opts(Idx, State) ->
    Opts = State#state.connection_opts,
    EventHandler = maps:with([event_handler], Opts),
    Opts#{
        retry => 0,
        supervise => false,
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
    ok = gun:shutdown(ConnectionPid).

%%

-spec handle_connection_started(connection_pid(), state()) -> state().
handle_connection_started(ConnectionPid, State) ->
    #connection_state{status = {starting, {ClientPid, _} = Requester, Locking}} =
        ConnSt = get_connection_state(ConnectionPid, State),
    ConnSt1 = ConnSt#connection_state{status = up, lock = unlocked},
    ConnSt2 = reset_connection_idle(ConnSt1),
    ok = reply_to_requester({ok, ConnectionPid}, Requester),
    State1 = set_connection_state(ConnectionPid, ConnSt2, State),
    State2 = dec_starting_count(inc_active_count(State1)),
    maybe_lock_connection(Locking, ConnectionPid, ClientPid, State2).

-spec reply_to_requester(term(), requester()) -> ok.
reply_to_requester(Message, Requester) ->
    _ = gen_server:reply(Requester, Message),
    ok.

%%

-spec handle_connection_down(connection_pid(), Reason :: _, state()) -> state().
handle_connection_down(ConnectionPid, Reason, State) ->
    ConnSt = get_connection_state(ConnectionPid, State),
    process_connection_removal(ConnSt, Reason, State).

-spec process_connection_removal(connection_state(), Reason :: _, state()) -> state().
process_connection_removal(ConnState = #connection_state{status = {starting, Requester, _Locking}}, Reason, State) ->
    ok = reply_to_requester({error, {connection_failed, Reason}}, Requester),
    State1 = free_connection_idx(ConnState#connection_state.idx, State),
    remove_connection(ConnState, dec_starting_count(State1));
process_connection_removal(ConnState = #connection_state{status = up}, _Reason, State) ->
    remove_up_connection(ConnState, State);
process_connection_removal(ConnState = #connection_state{status = down}, _Reason, State) ->
    remove_down_connection(ConnState, State).

-spec remove_up_connection(connection_state(), state()) -> state().
remove_up_connection(ConnState = #connection_state{idx = ConnIdx}, State) ->
    State1 = free_connection_idx(ConnIdx, State),
    remove_down_connection(ConnState, dec_active_count(State1)).

-spec remove_down_connection(connection_state(), state()) -> state().
remove_down_connection(ConnState = #connection_state{lock = unlocked}, State) ->
    remove_connection(ConnState, State);
remove_down_connection(ConnState = #connection_state{lock = {locked, ClientPid}}, State) ->
    State1 = remove_connection_from_client_state(ConnState#connection_state.pid, ClientPid, State),
    remove_connection(ConnState, State1).

-spec remove_connection(connection_state(), state()) -> state().
remove_connection(#connection_state{pid = ConnPid}, State) ->
    remove_connection_state(ConnPid, State).

-spec handle_client_down(client_pid(), mref(), Reason :: _, state()) -> state().
handle_client_down(ClientPid, Mref, _Reason, State) ->
    #client_state{mref = Mref, connections = Connections} = get_client_state(ClientPid, State),
    unlock_client_connections(Connections, State).

-spec unlock_client_connections([connection_pid()], state()) -> state().
unlock_client_connections([], State) ->
    State;
unlock_client_connections([ConnectionPid | Rest], State) ->
    State1 = do_unlock_connection(ConnectionPid, State),
    unlock_client_connections(Rest, State1).

%%

-spec handle_cleanup(state()) -> state().
handle_cleanup(State) ->
    Iter = maps:iterator(State#state.connections),
    process_connection_cleanup(maps:next(Iter), get_cleanup_size_budget(State), State).

-spec get_cleanup_size_budget(state()) -> integer().
get_cleanup_size_budget(State) ->
    %% TODO we need the total up connections here, excluding started ones
    State#state.active_count - State#state.min_size.

-spec process_connection_cleanup(next_conn(), integer(), state()) -> state().
process_connection_cleanup({Pid, ConnSt, Iter}, SizeBudget, State) when SizeBudget > 0 ->
    MaxAge = State#state.max_connection_idle_age,
    CurrentTime = erlang:monotonic_time(millisecond),
    ConnLoad = get_connection_load(ConnSt#connection_state.idx, State),
    case ConnSt of
        #connection_state{status = up, idle_since = IdleSince, idx = ConnIdx} when
            IdleSince + MaxAge =< CurrentTime, ConnLoad =:= 0
        ->
            %% Close connection, 'EXIT' msg will remove it from state
            ok = close_gun_connection(Pid),
            State1 = free_connection_idx(ConnIdx, State),
            NewConnectionSt = ConnSt#connection_state{status = down},
            State2 = set_connection_state(Pid, NewConnectionSt, dec_active_count(State1)),
            process_connection_cleanup(maps:next(Iter), SizeBudget - 1, State2);
        #connection_state{} ->
            process_connection_cleanup(maps:next(Iter), SizeBudget, State)
    end;
process_connection_cleanup(_NextConn, _SizeBudget, State) ->
    State.

-spec reset_connection_idle(connection_state()) -> connection_state().
reset_connection_idle(ConnectionSt) ->
    ConnectionSt#connection_state{idle_since = erlang:monotonic_time(millisecond)}.

%%

-spec maybe_lock_connection(locking(), connection_pid(), client_pid(), state()) -> state().
maybe_lock_connection(true, ConnectionPid, ClientPid, State) ->
    lock_connection(ConnectionPid, ClientPid, State);
maybe_lock_connection(false, _ConnectionPid, _ClientPid, State) ->
    State.

-spec lock_connection(connection_pid(), client_pid(), state()) -> state().
lock_connection(ConnectionPid, ClientPid, State) ->
    State1 = do_lock_connection(ConnectionPid, ClientPid, State),
    ClientSt = get_or_create_client_state(ClientPid, State1),
    ClientSt1 = do_add_connection_to_client(ConnectionPid, ClientSt),
    set_client_state(ClientPid, ClientSt1, State1).

-spec do_lock_connection(connection_pid(), client_pid(), state()) -> state().
do_lock_connection(ConnectionPid, ClientPid, State) ->
    ConnectionSt = get_connection_state(ConnectionPid, State),
    ConnectionSt1 = ConnectionSt#connection_state{status = up, lock = {locked, ClientPid}},
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
    ConnectionSt = get_connection_state(ConnectionPid, State),
    ConnectionSt1 = ConnectionSt#connection_state{status = up, lock = unlocked},
    set_connection_state(ConnectionPid, ConnectionSt1, State).

-spec remove_connection_from_client_state(connection_pid(), client_pid(), state()) -> state().
remove_connection_from_client_state(ConnectionPid, ClientPid, State) ->
    ClientSt = get_client_state(ClientPid, State),
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
        #client_state{} = ClientSt ->
            ClientSt;
        undefined ->
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

-spec get_client_state(client_pid(), state()) -> client_state() | undefined.
get_client_state(Pid, State) ->
    maps:get(Pid, State#state.clients, undefined).

-spec set_client_state(client_pid(), client_state(), state()) -> state().
set_client_state(Pid, ClientSt, State) ->
    State#state{clients = maps:put(Pid, ClientSt, State#state.clients)}.

-spec remove_client_state(client_pid(), state()) -> state().
remove_client_state(Pid, State) ->
    State#state{clients = maps:remove(Pid, State#state.clients)}.

%%

-spec inc_active_count(state()) -> state().
inc_active_count(State) ->
    offset_active_count(State, 1).

-spec dec_active_count(state()) -> state().
dec_active_count(State) ->
    offset_active_count(State, -1).

-spec offset_active_count(state(), integer()) -> state().
offset_active_count(State, Inc) ->
    State#state{active_count = State#state.active_count + Inc}.

-spec inc_starting_count(state()) -> state().
inc_starting_count(State) ->
    offset_starting_count(State, 1).

-spec dec_starting_count(state()) -> state().
dec_starting_count(State) ->
    offset_starting_count(State, -1).

-spec offset_starting_count(state(), integer()) -> state().
offset_starting_count(State, Inc) ->
    State#state{starting_count = State#state.starting_count + Inc}.

-spec create_pool_status_response(state()) -> pool_status_response().
create_pool_status_response(State) ->
    #{
        total_connections => State#state.active_count + State#state.starting_count,
        available_connections => get_available_connections_count(State#state.connections)
    }.

-spec get_available_connections_count(connections()) -> size().
get_available_connections_count(Connections) ->
    maps:fold(
        fun
            (_K, #connection_state{status = up, lock = unlocked}, Acc) -> Acc + 1;
            (_K, #connection_state{}, Acc) -> Acc
        end,
        0,
        Connections
    ).
