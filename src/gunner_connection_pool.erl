%% Handles placing connections in groups, acquiring connections from connection groups
%% Manages free and total connection counters
%% Guarantees freeing only busy connections

-module(gunner_connection_pool).

%% API functions

-export([new/0]).

-export([total_count/1]).
-export([free_count/1]).
-export([group_count/2]).

-export([is_busy/2]).

-export([has_connection/2]).
-export([has_connection/3]).

-export([get_assignment/2]).

-export([assign/3]).
-export([remove/3]).

-export([acquire/2]).
-export([free/3]).

%% API Types

-type state() :: #{
    total_connections := size(),
    free_connections := size(),
    connection_groups := connection_groups(),
    assignments := assignments(),
    busy_connections := [connection()]
}.

-export_type([state/0]).

%% Internal types

-type group_id() :: term().
-type connection_group() :: gunner_connection_pool_group:state().
-type connection_groups() :: #{group_id() => connection_group()}.

-type assignments() :: #{connection() => group_id()}.

-type connection() :: gunner:connection().
-type size() :: non_neg_integer().

%%
%% API functions
%%

-spec new() -> state().
new() ->
    #{
        total_connections => 0,
        free_connections => 0,
        connection_groups => #{},
        assignments => #{},
        busy_connections => []
    }.

-spec total_count(state()) -> size().
total_count(#{total_connections := TotalCount}) ->
    TotalCount.

-spec free_count(state()) -> size().
free_count(#{free_connections := FreeCount}) ->
    FreeCount.

-spec group_count(group_id(), state()) -> size() | {error, group_not_found}.
group_count(GroupID, State) ->
    {Result, _} = with_group(GroupID, State, fun(GroupSt0) ->
        Size = gunner_connection_pool_group:size(GroupSt0),
        {Size, GroupSt0}
    end),
    Result.

-spec is_busy(connection(), state()) -> boolean().
is_busy(Connection, State) ->
    is_connection_busy(Connection, State).

-spec has_connection(connection(), state()) -> boolean().
has_connection(Connection, State) ->
    case is_connection_busy(Connection, State) of
        true ->
            true;
        false ->
            #{connection_groups := Groups} = State,
            lists:any(fun(GroupID) -> has_connection(Connection, GroupID, State) end, maps:keys(Groups))
    end.

-spec has_connection(connection(), group_id(), state()) -> boolean() | {error, group_not_found}.
has_connection(Connection, GroupID, State) ->
    {Result, _} = with_group(GroupID, State, fun(GroupSt0) ->
        Has = gunner_connection_pool_group:has_connection(Connection, GroupSt0),
        {Has, GroupSt0}
    end),
    Result.

-spec get_assignment(connection(), state()) -> {ok, group_id()} | {error, no_connection}.
get_assignment(Connection, State) ->
    case get_connection_assignment(Connection, State) of
        GroupID when GroupID =/= undefined ->
            {ok, GroupID};
        undefined ->
            {error, no_connection}
    end.

%%

-spec assign(connection(), group_id(), state()) -> {ok, state()} | {error, connection_exists}.
assign(Connection, GroupID, State0) ->
    State = ensure_group_exists(GroupID, State0),
    {Result, State1} = with_group(GroupID, State, fun(GroupSt0) ->
        case gunner_connection_pool_group:has_connection(Connection, GroupSt0) of
            false ->
                GroupSt1 = gunner_connection_pool_group:add_connection(Connection, GroupSt0),
                {ok, GroupSt1};
            true ->
                {{error, connection_exists}, GroupSt0}
        end
    end),
    case Result of
        ok ->
            State2 = set_connection_assignment(Connection, GroupID, State1),
            {ok, counters_added_connection(State2)};
        {error, connection_exists} ->
            {error, connection_exists}
    end.

-spec remove(connection(), group_id(), state()) -> {ok, state()} | {error, connection_busy} | {error, group_not_found}.
remove(Connection, GroupID, State0) ->
    case is_connection_busy(Connection, State0) of
        false ->
            {Result, State1} = with_group(GroupID, State0, fun(GroupSt0) ->
                case gunner_connection_pool_group:has_connection(Connection, GroupSt0) of
                    true ->
                        GroupSt1 = gunner_connection_pool_group:remove_connection(Connection, GroupSt0),
                        {true, GroupSt1};
                    false ->
                        %% It already does not exist so mission accomplished
                        {false, GroupSt0}
                end
            end),
            case Result of
                true ->
                    State2 = clear_connection_assignment(Connection, State1),
                    {ok, counters_removed_connection(State2)};
                false ->
                    {ok, State1};
                {error, group_not_found} = Error ->
                    Error
            end;
        true ->
            %% Removing busy connections is undefined
            {error, connection_busy}
    end.

%%

-spec acquire(group_id(), state()) -> {ok, connection(), state()} | {ok, no_connection} | {error, group_not_found}.
acquire(GroupID, State0) ->
    {Result, State1} = with_group(GroupID, State0, fun(GroupSt0) ->
        case gunner_connection_pool_group:next_connection(GroupSt0) of
            {ok, Connection, GroupSt1} ->
                {{ok, Connection}, GroupSt1};
            {error, empty} ->
                {{ok, no_connection}, GroupSt0}
        end
    end),
    case Result of
        {ok, no_connection} ->
            {ok, no_connection};
        {ok, Connection} ->
            State2 = set_connection_busy(Connection, State1),
            {ok, Connection, counters_acquired_connection(State2)};
        {error, group_not_found} ->
            {error, group_not_found}
    end.

-spec free(connection(), group_id(), state()) -> {ok, state()} | {error, invalid_connection} | {error, group_not_found}.
free(Connection, GroupID, State0) ->
    case is_connection_busy(Connection, State0) of
        true ->
            {ok, State1} = with_group(GroupID, State0, fun(GroupSt0) ->
                GroupSt1 = gunner_connection_pool_group:add_connection(Connection, GroupSt0),
                {ok, GroupSt1}
            end),
            State2 = clear_connection_busy(Connection, State1),
            {ok, counters_freed_connection(State2)};
        false ->
            {error, invalid_connection}
    end.

%%
%% Internal functions
%%

group_exists(GroupID, #{connection_groups := Groups}) ->
    maps:is_key(GroupID, Groups).

ensure_group_exists(GroupID, St = #{connection_groups := Groups}) ->
    case group_exists(GroupID, St) of
        true ->
            St;
        false ->
            St#{connection_groups => save_group_state(GroupID, gunner_connection_pool_group:new(), Groups)}
    end.

%%

is_connection_busy(Connection, #{busy_connections := BusyConnections}) ->
    lists:member(Connection, BusyConnections).

set_connection_busy(Connection, St = #{busy_connections := BusyConnections}) ->
    St#{busy_connections => [Connection | BusyConnections]}.

clear_connection_busy(Connection, St = #{busy_connections := BusyConnections}) ->
    St#{busy_connections => lists:delete(Connection, BusyConnections)}.

%%

get_connection_assignment(Connection, #{assignments := Assignments}) ->
    maps:get(Connection, Assignments, undefined).

set_connection_assignment(Connection, GroupID, St = #{assignments := Assignments}) ->
    St#{assignments => Assignments#{Connection => GroupID}}.

clear_connection_assignment(Connection, St = #{assignments := Assignments}) ->
    St#{assignments => maps:without([Connection], Assignments)}.

%%

counters_added_connection(St0 = #{free_connections := FreeAmount, total_connections := TotalAmount}) ->
    St0#{free_connections => FreeAmount + 1, total_connections => TotalAmount + 1}.

counters_removed_connection(St0 = #{free_connections := FreeAmount, total_connections := TotalAmount}) when
    FreeAmount > 0, TotalAmount > 0
->
    St0#{free_connections => FreeAmount - 1, total_connections => TotalAmount - 1}.

counters_acquired_connection(St0 = #{free_connections := FreeAmount}) when FreeAmount > 0 ->
    St0#{free_connections => FreeAmount - 1}.

counters_freed_connection(St0 = #{free_connections := FreeAmount}) ->
    St0#{free_connections => FreeAmount + 1}.

%%

save_group_state(GroupID, GroupSt, Groups) ->
    Groups#{GroupID => GroupSt}.

-spec with_group(group_id(), state(), fun((connection_group()) -> {Result, connection_group()})) ->
    {Result, state()}
when
    Result :: {error, group_not_found} | any().
with_group(GroupID, St = #{connection_groups := Groups}, Fun) ->
    case maps:get(GroupID, Groups, undefined) of
        GroupSt when GroupSt =/= undefined ->
            case Fun(GroupSt) of
                {Result, GroupSt} ->
                    {Result, St};
                {Result, NewGroupSt} ->
                    {Result, St#{connection_groups => save_group_state(GroupID, NewGroupSt, Groups)}}
            end;
        undefined ->
            {{error, group_not_found}, St}
    end.

-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

-spec assign_test() -> _.

assign_test() ->
    St0 = new(),
    {ok, St1} = assign(connection_1, group_1, St0),
    ?assert(has_connection(connection_1, group_1, St1)),
    ?assert(has_connection(connection_1, St1)),
    ?assertEqual({error, connection_exists}, assign(connection_1, group_1, St1)),
    ?assertEqual(1, total_count(St1)),
    ?assertEqual(1, free_count(St1)),
    ?assertEqual(1, group_count(group_1, St1)).

-spec get_assignment_test() -> _.
get_assignment_test() ->
    St0 = new(),
    {ok, St1} = assign(connection_1, group_1, St0),
    ?assertEqual({ok, group_1}, get_assignment(connection_1, St1)),
    ?assertEqual({error, no_connection}, get_assignment(connection_2, St1)),
    {ok, St2} = remove(connection_1, group_1, St1),
    {ok, St3} = assign(connection_2, group_2, St2),
    ?assertEqual({error, no_connection}, get_assignment(connection_1, St3)),
    ?assertEqual({ok, group_2}, get_assignment(connection_2, St3)).

-spec remove_test() -> _.
remove_test() ->
    St0 = new(),
    {ok, St1} = assign(connection_1, group_1, St0),
    {ok, connection_1, St2} = acquire(group_1, St1),
    ?assertEqual({error, connection_busy}, remove(connection_1, group_1, St2)),
    {ok, St3} = free(connection_1, group_1, St2),
    {ok, St4} = remove(connection_1, group_1, St3),
    ?assertEqual({ok, St4}, remove(connection_1, group_1, St4)),
    ?assertEqual({error, group_not_found}, remove(connection_1, group_2, St4)).

-spec acquire_test() -> _.
acquire_test() ->
    St0 = new(),
    {ok, St1} = assign(connection_1, group_1, St0),
    {ok, St2} = assign(connection_2, group_1, St1),
    {ok, connection_1, St3} = acquire(group_1, St2),
    {ok, connection_2, St4} = acquire(group_1, St3),
    {ok, no_connection} = acquire(group_1, St4),
    ?assertEqual({error, group_not_found}, acquire(group_2, St4)).

-spec free_test() -> _.
free_test() ->
    St0 = new(),
    {ok, St1} = assign(connection_1, group_1, St0),
    {ok, St2} = assign(connection_2, group_1, St1),
    {ok, connection_1, St3} = acquire(group_1, St2),
    {ok, connection_2, St4} = acquire(group_1, St3),
    {ok, St5} = free(connection_1, group_1, St4),
    ?assertEqual({error, invalid_connection}, free(connection_1, group_1, St5)),
    {ok, St6} = free(connection_2, group_1, St5),
    ?assertEqual({error, invalid_connection}, free(connection_1, group_2, St6)).

-spec counters_test() -> _.
counters_test() ->
    St0 = new(),
    {ok, St1} = assign(connection_1, group_1, St0),
    {ok, St2} = assign(connection_2, group_2, St1),
    ?assertEqual(2, total_count(St2)),
    ?assertEqual(2, free_count(St2)),
    ?assertEqual(1, group_count(group_1, St2)),
    ?assertEqual(1, group_count(group_2, St2)),
    {ok, connection_1, St3} = acquire(group_1, St2),
    ?assertEqual(2, total_count(St3)),
    ?assertEqual(1, free_count(St3)),
    ?assertEqual(0, group_count(group_1, St3)),
    ?assertEqual(1, group_count(group_2, St3)),
    {ok, connection_2, St4} = acquire(group_2, St3),
    ?assertEqual(2, total_count(St4)),
    ?assertEqual(0, free_count(St4)),
    ?assertEqual(0, group_count(group_1, St4)),
    ?assertEqual(0, group_count(group_2, St4)),
    {ok, St5} = free(connection_1, group_1, St4),
    ?assertEqual(2, total_count(St5)),
    ?assertEqual(1, free_count(St5)),
    ?assertEqual(1, group_count(group_1, St5)),
    ?assertEqual(0, group_count(group_2, St5)),
    {ok, St6} = free(connection_2, group_2, St5),
    ?assertEqual(2, total_count(St6)),
    ?assertEqual(2, free_count(St6)),
    ?assertEqual(1, group_count(group_1, St6)),
    ?assertEqual(1, group_count(group_2, St6)),
    {ok, St7} = remove(connection_1, group_1, St6),
    ?assertEqual(1, total_count(St7)),
    ?assertEqual(1, free_count(St7)),
    ?assertEqual(0, group_count(group_1, St7)),
    ?assertEqual(1, group_count(group_2, St7)),
    {ok, St8} = remove(connection_2, group_2, St7),
    ?assertEqual(0, total_count(St8)),
    ?assertEqual(0, free_count(St8)),
    ?assertEqual(0, group_count(group_1, St8)),
    ?assertEqual(0, group_count(group_2, St8)).

-endif.
