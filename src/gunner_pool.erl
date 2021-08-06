-module(gunner_pool).

-include("gunner_events.hrl").

%% API functions

-export([start_pool/1]).
-export([start_pool/2]).
-export([stop_pool/1]).
-export([start_link/2]).

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
%% to satisfy an 'acquire' {pool, unavailable} will be returned instead.
-type max_size() :: size().

%% @doc Mininum amount of connections kept in pool. This is a soft limit, so connections must be first opened
%% naturally by 'acquire' requests. When pool size is at this value, no more connections are killed by
%% cleanup operations.
-type min_size() :: size().

%% @doc Gun opts used when creating new connections in this pool. See `gun:opts/0`.
-type connection_opts() :: gun:opts().

%% @doc Event handler for the pool.
-type event_handler() :: gunner_event_h:handler().

-type pool_opts() :: #{
    cleanup_interval => cleanup_interval(),
    max_connection_load => max_connection_load(),
    max_connection_idle_age => max_connection_idle_age(),
    max_size => max_size(),
    min_size => min_size(),
    connection_opts => connection_opts(),
    event_handler => event_handler()
}.

-type group_id() :: term().

-type pool_status_response() :: #{total_connections := size(), available_connections := size()}.

-type acquire_error() ::
    {pool, not_found | unavailable} |
    {connection_failed, _}.

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
-export_type([event_handler/0]).

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
    cleanup_interval :: cleanup_interval(),
    event_handler :: event_handler()
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

-define(DEFAULT_EVENT_HANDLER, {gunner_default_event_h, undefined}).

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

-spec start_pool(pool_reg_name() | undefined, pool_opts()) -> {ok, pid()} | {error, {pool, already_exists}}.
start_pool(PoolRegName, PoolOpts) ->
    case gunner_pool_sup:start_pool(PoolRegName, PoolOpts) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, _}} ->
            {error, {pool, already_exists}}
    end.

-spec stop_pool(pid()) -> ok | {error, {pool, not_found}}.
stop_pool(Pid) ->
    case gunner_pool_sup:stop_pool(Pid) of
        ok ->
            ok;
        {error, not_found} ->
            {error, {pool, not_found}}
    end.

-spec start_link(pool_reg_name() | undefined, pool_opts()) -> genlib_gen:start_ret().
start_link(undefined, PoolOpts) ->
    gen_server:start_link(?MODULE, [PoolOpts], []);
start_link(PoolRegName, PoolOpts) ->
    gen_server:start_link(PoolRegName, ?MODULE, [PoolOpts], []).

%%

-spec acquire(pool_id(), endpoint(), timeout()) ->
    {ok, connection_pid()} |
    {error, acquire_error() | {call, timeout}}.
acquire(PoolID, Endpoint, Timeout) ->
    call_pool(PoolID, {acquire, Endpoint, false}, Timeout).

-spec acquire(pool_id(), endpoint(), locking(), timeout()) ->
    {ok, connection_pid()} |
    {error, acquire_error() | {call, timeout}}.
acquire(PoolID, Endpoint, Locking, Timeout) ->
    call_pool(PoolID, {acquire, Endpoint, Locking}, Timeout).

-spec free(pool_id(), connection_pid()) -> ok.
free(PoolID, ConnectionPid) ->
    gen_server:cast(PoolID, {free, ConnectionPid, self()}).

%% API helpers

-spec call_pool(pool_id(), Args :: _, timeout()) -> Response :: _ | no_return().
call_pool(PoolRef, Args, Timeout) ->
    try
        gen_server:call(PoolRef, Args, Timeout)
    catch
        exit:{noproc, _} ->
            {error, {pool, not_found}};
        exit:{timeout, _} ->
            {error, {call, timeout}}
    end.

%%
%% Gen Server callbacks
%%

-spec init(list()) -> {ok, state()}.
init([InitOpts]) ->
    Opts = maps:merge(default_opts(), InitOpts),
    State = new_state(Opts),
    _ = erlang:process_flag(trap_exit, true),
    _ = erlang:send_after(State#state.cleanup_interval, self(), ?GUNNER_CLEANUP()),
    State1 = emit_init_event(Opts, State),
    {ok, State1}.

-spec handle_call
    ({acquire, endpoint(), locking()}, from(), state()) ->
        {noreply, state()} |
        {reply, {ok, connection_pid()} | {error, {pool, unavailable} | {connection_failed, Reason :: _}}, state()};
    (status, from(), state()) -> {reply, {ok, pool_status_response()}, state()}.
handle_call({acquire, Endpoint, Locking}, {ClientPid, _} = From, State0) ->
    GroupID = create_group_id(Endpoint),
    State1 = emit_acquire_started_event(GroupID, ClientPid, Locking, State0),
    case handle_acquire_connection(Endpoint, GroupID, Locking, From, State1) of
        {{ok, {ready, Connection}}, NewState} ->
            NewState1 = emit_acquire_finished_event({ok, Connection}, GroupID, ClientPid, NewState),
            {reply, {ok, Connection}, NewState1};
        {{ok, {started, Connection}}, NewState} ->
            NewState1 = emit_connection_init_started_event(Connection, NewState),
            {noreply, NewState1};
        {error, Reason} ->
            State2 = emit_acquire_finished_event({error, Reason}, GroupID, ClientPid, State1),
            {reply, {error, Reason}, State2}
    end.

-spec handle_cast({free, connection_pid(), client_pid()}, state()) -> {noreply, state()}.
handle_cast({free, ConnectionPid, ClientPid}, State0) ->
    %% Assuming here that the pids we receive are anything close to the ones we
    %% have in our state is probably not the way to go
    case get_connection_state(ConnectionPid, State0) of
        #connection_state{} = ConnSt ->
            State1 = emit_free_started_event(ConnSt, ClientPid, State0),
            case handle_free_connection(ConnSt, ClientPid, State1) of
                {ok, NewState} ->
                    NewState1 = emit_free_finished_event(ok, ConnSt, ClientPid, NewState),
                    {noreply, NewState1};
                not_locked ->
                    State2 = emit_free_finished_event({error, not_locked}, ConnSt, ClientPid, State1),
                    {noreply, State2}
            end;
        _ ->
            State1 = emit_free_error_event(connection_not_found, ClientPid, State0),
            {noreply, State1}
    end.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info(?GUNNER_CLEANUP(), State0) ->
    State1 = emit_cleanup_started_event(State0),
    State2 = handle_cleanup(State1),
    _ = erlang:send_after(State2#state.cleanup_interval, self(), ?GUNNER_CLEANUP()),
    State3 = emit_cleanup_finished_event(State2),
    {noreply, State3};
handle_info(?GUN_UP(ConnectionPid), State0) ->
    State1 = emit_connection_init_finished_event(ok, ConnectionPid, State0),
    State2 = handle_connection_started(ConnectionPid, State1),
    {noreply, State2};
handle_info(?GUN_DOWN(_ConnectionPid, _Reason), State) ->
    {noreply, State};
handle_info(?DOWN(Mref, ClientPid, Reason), State) ->
    State1 = handle_client_down(ClientPid, Mref, Reason, State),
    State2 = emit_client_down_event(ClientPid, Reason, State1),
    {noreply, State2};
handle_info(?EXIT(ConnectionPid, Reason), State) ->
    State1 = handle_connection_down(ConnectionPid, Reason, State),
    {noreply, State1}.

-spec terminate(any(), state()) -> ok.
terminate(Reason, State) ->
    _ = emit_terminate_event(Reason, State),
    ok.

%%

-spec default_opts() -> pool_opts().
default_opts() ->
    #{
        cleanup_interval => ?DEFAULT_CLEANUP_INTERVAL,
        max_connection_load => ?DEFAULT_MAX_CONNECTION_LOAD,
        max_connection_idle_age => ?DEFAULT_MAX_CONNECTION_IDLE_AGE,
        max_size => ?DEFAULT_MAX_SIZE,
        min_size => ?DEFAULT_MIN_SIZE,
        connection_opts => ?DEFAULT_CONNECTION_OPTS,
        event_handler => ?DEFAULT_EVENT_HANDLER
    }.

-spec new_state(pool_opts()) -> state().
new_state(Opts) ->
    MaxSize = maps:get(max_size, Opts),
    #state{
        max_size = MaxSize,
        min_size = maps:get(min_size, Opts),
        active_count = 0,
        starting_count = 0,
        max_connection_load = maps:get(max_connection_load, Opts),
        idx_authority = gunner_idx_authority:new(MaxSize),
        counters_ref = counters:new(MaxSize, [atomics]),
        connections = #{},
        connection_opts = maps:get(connection_opts, Opts),
        cleanup_interval = maps:get(cleanup_interval, Opts),
        max_connection_idle_age = maps:get(max_connection_idle_age, Opts),
        event_handler = maps:get(event_handler, Opts)
    }.

%%

-spec handle_acquire_connection(endpoint(), group_id(), locking(), requester(), state()) ->
    {Result, state()} | Error
when
    Result :: {ok, {started | ready, connection_pid()}},
    Error :: {error, {connection_failed, _Reason} | {pool, unavailable}}.
handle_acquire_connection(Endpoint, GroupID, Locking, {ClientPid, _} = From, State) ->
    case acquire_connection_from_group(GroupID, State) of
        {connection, ConnPid, St1} ->
            {{ok, {ready, ConnPid}}, maybe_lock_connection(Locking, ConnPid, ClientPid, St1)};
        no_connection ->
            case handle_connection_creation(GroupID, Endpoint, From, Locking, State) of
                {ok, Connection, State1} ->
                    {{ok, {started, Connection}}, State1};
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

-spec handle_free_connection(connection_state(), client_pid(), state()) -> {ok, state()} | not_locked.
handle_free_connection(#connection_state{status = up, lock = {locked, ClientPid}} = ConnSt, ClientPid, State) ->
    ConnSt1 = reset_connection_idle(ConnSt),
    State1 = do_unlock_connection(ConnSt1, ClientPid, State),
    State2 = remove_connection_from_client_state(ConnSt1#connection_state.pid, ClientPid, State1),
    {ok, State2};
handle_free_connection(_, _ClientPid, _State) ->
    not_locked.

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
    {ok, connection_pid(), state()} | {error, {pool, unavailable} | {connection_failed, Reason :: _}}.
handle_connection_creation(GroupID, Endpoint, Requester, Locking, State) ->
    case is_pool_available(State) of
        true ->
            {Idx, State1} = new_connection_idx(State),
            case open_gun_connection(Endpoint, Idx, State) of
                {ok, Pid} ->
                    ConnectionState = new_connection_state(Requester, GroupID, Idx, Pid, Locking),
                    {ok, Pid, set_connection_state(Pid, ConnectionState, inc_starting_count(State1))};
                {error, Reason} ->
                    {error, {connection_failed, Reason}}
            end;
        false ->
            {error, {pool, unavailable}}
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
    #connection_state{group_id = GroupID, status = {starting, {ClientPid, _} = Requester, Locking}} =
        ConnSt = get_connection_state(ConnectionPid, State),
    ConnSt1 = ConnSt#connection_state{status = up, lock = unlocked},
    ConnSt2 = reset_connection_idle(ConnSt1),
    ok = reply_to_requester({ok, ConnectionPid}, Requester),
    State1 = set_connection_state(ConnectionPid, ConnSt2, State),
    State2 = dec_starting_count(inc_active_count(State1)),
    State3 = maybe_lock_connection(Locking, ConnectionPid, ClientPid, State2),
    emit_acquire_finished_event({ok, ConnectionPid}, GroupID, ClientPid, State3).

-spec reply_to_requester(term(), requester()) -> ok.
reply_to_requester(Message, Requester) ->
    _ = gen_server:reply(Requester, Message),
    ok.

%%

-spec handle_connection_down(connection_pid(), Reason :: _, state()) -> state().
handle_connection_down(ConnectionPid, Reason, State) ->
    ConnSt = get_connection_state(ConnectionPid, State),
    %% I want to rewrite all of this so bad
    process_connection_removal(ConnSt, Reason, State).

-spec process_connection_removal(connection_state(), Reason :: _, state()) -> state().
process_connection_removal(
    ConnState = #connection_state{group_id = GroupID, status = {starting, Requester, _Locking}},
    Reason,
    State
) ->
    ok = reply_to_requester({error, {connection_failed, Reason}}, Requester),
    State1 = free_connection_idx(ConnState#connection_state.idx, State),
    State2 = emit_connection_init_finished_event(
        {error, {connection_failed, Reason}},
        ConnState#connection_state.pid,
        State1
    ),
    {ClientPid, _} = Requester,
    State3 = emit_acquire_finished_event({error, {connection_failed, Reason}}, GroupID, ClientPid, State2),
    remove_connection(ConnState, dec_starting_count(State3));
process_connection_removal(ConnState = #connection_state{status = up}, Reason, State) ->
    State1 = emit_connection_down_event({abnormal, Reason}, ConnState#connection_state.pid, State),
    remove_up_connection(ConnState, State1);
process_connection_removal(ConnState = #connection_state{status = down}, _Reason, State) ->
    State1 = emit_connection_down_event(normal, ConnState#connection_state.pid, State),
    remove_down_connection(ConnState, State1).

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
    unlock_client_connections(Connections, ClientPid, State).

-spec unlock_client_connections([connection_pid()], client_pid(), state()) -> state().
unlock_client_connections([], _ClientPid, State) ->
    State;
unlock_client_connections([ConnectionPid | Rest], ClientPid, State) ->
    ConnectionSt = get_connection_state(ConnectionPid, State),
    State1 = do_unlock_connection(ConnectionSt, ClientPid, State),
    unlock_client_connections(Rest, ClientPid, State1).

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
        %% Locked connections should not be cleaned up when unused
        #connection_state{status = up, lock = unlocked, idle_since = IdleSince, idx = ConnIdx} when
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
    State2 = set_client_state(ClientPid, ClientSt1, State1),
    emit_connection_locked_event(ConnectionPid, ClientPid, State2).

-spec do_lock_connection(connection_pid(), client_pid(), state()) -> state().
do_lock_connection(ConnectionPid, ClientPid, State) ->
    ConnectionSt = get_connection_state(ConnectionPid, State),
    ConnectionSt1 = ConnectionSt#connection_state{status = up, lock = {locked, ClientPid}},
    set_connection_state(ConnectionPid, ConnectionSt1, State).

-spec do_add_connection_to_client(connection_pid(), client_state()) -> client_state().
do_add_connection_to_client(ConnectionPid, ClientSt = #client_state{connections = Connections}) ->
    ClientSt#client_state{connections = [ConnectionPid | Connections]}.

-spec do_unlock_connection(connection_state(), client_pid(), state()) -> state().
do_unlock_connection(ConnectionSt, ClientPid, State) ->
    ConnectionSt1 = ConnectionSt#connection_state{status = up, lock = unlocked},
    State1 = set_connection_state(ConnectionSt1#connection_state.pid, ConnectionSt1, State),
    emit_connection_unlocked_event(ConnectionSt1, ClientPid, State1).

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

%%

emit_init_event(PoolOpts, State) ->
    emit_event(
        #gunner_pool_init_event{
            pool_opts = PoolOpts
        },
        State
    ).

emit_terminate_event(Reason, State) ->
    emit_event(
        #gunner_pool_terminate_event{
            reason = Reason
        },
        State
    ).

emit_acquire_started_event(GroupID, Client, Locking, State) ->
    emit_event(
        #gunner_acquire_started_event{
            group_id = GroupID,
            client = Client,
            is_locking = Locking
        },
        State
    ).

emit_acquire_finished_event({ok, ConnectionPid}, GroupID, Client, State) ->
    emit_event(
        #gunner_acquire_finished_event{
            result = ok,
            connection = ConnectionPid,
            group_id = GroupID,
            client = Client
        },
        State
    );
emit_acquire_finished_event(Result = {error, _}, GroupID, Client, State) ->
    emit_event(
        #gunner_acquire_finished_event{
            result = Result,
            group_id = GroupID,
            client = Client
        },
        State
    ).

emit_connection_locked_event(ConnectionPid, ClientPid, State) ->
    ConnSt = get_connection_state(ConnectionPid, State),
    emit_event(
        #gunner_connection_locked_event{
            connection = ConnectionPid,
            group_id = ConnSt#connection_state.group_id,
            client = ClientPid
        },
        State
    ).

emit_connection_unlocked_event(ConnSt, ClientPid, State) ->
    emit_event(
        #gunner_connection_unlocked_event{
            connection = ConnSt#connection_state.pid,
            group_id = ConnSt#connection_state.group_id,
            client = ClientPid
        },
        State
    ).

emit_free_started_event(ConnSt, ClientPid, State) ->
    emit_event(
        #gunner_free_started_event{
            connection = ConnSt#connection_state.pid,
            group_id = ConnSt#connection_state.group_id,
            client = ClientPid
        },
        State
    ).

emit_free_finished_event(Result, ConnSt, ClientPid, State) ->
    emit_event(
        #gunner_free_finished_event{
            connection = ConnSt#connection_state.pid,
            group_id = ConnSt#connection_state.group_id,
            client = ClientPid,
            result = Result
        },
        State
    ).

emit_free_error_event(Reason, ClientPid, State) ->
    emit_event(
        #gunner_free_error_event{
            client = ClientPid,
            reason = Reason
        },
        State
    ).

emit_cleanup_started_event(State) ->
    emit_event(
        #gunner_cleanup_started_event{
            active_connections = State#state.active_count
        },
        State
    ).

emit_cleanup_finished_event(State) ->
    emit_event(
        #gunner_cleanup_finished_event{
            active_connections = State#state.active_count
        },
        State
    ).

emit_client_down_event(ClientPid, Reason, State) ->
    emit_event(
        #gunner_client_down_event{
            client = ClientPid,
            reason = Reason
        },
        State
    ).

emit_connection_init_started_event(ConnectionPid, State) ->
    ConnState = get_connection_state(ConnectionPid, State),
    emit_event(
        #gunner_connection_init_started_event{
            connection = ConnectionPid,
            group_id = ConnState#connection_state.group_id
        },
        State
    ).

emit_connection_init_finished_event(Result, ConnectionPid, State) ->
    ConnState = get_connection_state(ConnectionPid, State),
    emit_event(
        #gunner_connection_init_finished_event{
            result = Result,
            connection = ConnectionPid,
            group_id = ConnState#connection_state.group_id
        },
        State
    ).

emit_connection_down_event(Reason, ConnectionPid, State) ->
    ConnState = get_connection_state(ConnectionPid, State),
    emit_event(
        #gunner_connection_down_event{
            connection = ConnectionPid,
            group_id = ConnState#connection_state.group_id,
            reason = Reason
        },
        State
    ).

emit_event(EventData, State) ->
    State#state{event_handler = gunner_event_h:handle_event(EventData, State#state.event_handler)}.
