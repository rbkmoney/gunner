-module(gunner_connection_pool_group).

%% API functions

-export([new/0]).
-export([is_empty/1]).
-export([size/1]).

-export([add_connection/2]).
-export([remove_connection/2]).
-export([has_connection/2]).

-export([next_connection/1]).

%% API Types

-type state() :: queue:queue().

-export_type([state/0]).

%% Internal types

-type connection() :: gunner:connection().

%%
%% API functions
%%

-spec new() -> state().
new() ->
    queue:new().

-spec is_empty(state()) -> boolean().
is_empty(GroupState) ->
    queue:is_empty(GroupState).

-spec size(state()) -> non_neg_integer().
size(GroupState) ->
    %% @TODO maybe calculate and store size in state via add/delete
    queue:len(GroupState).

-spec add_connection(connection(), state()) -> state().
add_connection(Connection, GroupState) ->
    queue:in(Connection, GroupState).

-spec remove_connection(connection(), state()) -> state().
remove_connection(Connection, GroupState) ->
    queue:filter(fun(W) -> Connection =/= W end, GroupState).

-spec has_connection(connection(), state()) -> boolean().
has_connection(Connection, GroupState) ->
    queue:member(Connection, GroupState).

-spec next_connection(state()) -> {ok, connection(), state()} | {error, empty}.
next_connection(GroupState0) ->
    case queue:out(GroupState0) of
        {{value, Connection}, GroupState1} ->
            {ok, Connection, GroupState1};
        {empty, GroupState0} ->
            {error, empty}
    end.

-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

-spec size_test() -> _.

size_test() ->
    St0 = new(),
    ?assertEqual(0, ?MODULE:size(St0)),
    St1 = add_connection(connection_1, St0),
    ?assertEqual(1, ?MODULE:size(St1)),
    St2 = remove_connection(connection_1, St1),
    ?assertEqual(0, ?MODULE:size(St2)).

-spec is_empty_test() -> _.
is_empty_test() ->
    St0 = new(),
    ?assert(is_empty(St0)),
    St1 = add_connection(connection_1, St0),
    ?assertNot(is_empty(St1)),
    St2 = remove_connection(connection_1, St1),
    ?assert(is_empty(St2)).

-spec has_connection_test() -> _.
has_connection_test() ->
    St0 = new(),
    St1 = add_connection(connection_1, St0),
    St2 = add_connection(connection_2, St1),
    St3 = add_connection(connection_3, St2),
    ?assert(has_connection(connection_2, St3)),
    ?assertNot(has_connection(connection_4, St3)),
    St4 = remove_connection(connection_2, St3),
    St5 = add_connection(connection_4, St4),
    ?assertNot(has_connection(connection_2, St5)),
    ?assert(has_connection(connection_4, St5)).

-spec next_connection_test() -> _.
next_connection_test() ->
    St0 = new(),
    St1 = add_connection(connection_1, St0),
    St2 = add_connection(connection_2, St1),
    St3 = add_connection(connection_3, St2),
    {ok, Conn1, St4} = next_connection(St3),
    ?assertEqual(connection_1, Conn1),
    St5 = add_connection(connection_4, St4),
    {ok, Conn2, St6} = next_connection(St5),
    ?assertEqual(connection_2, Conn2),
    {ok, Conn3, St7} = next_connection(St6),
    ?assertEqual(connection_3, Conn3),
    {ok, Conn4, _St8} = next_connection(St7),
    ?assertEqual(connection_4, Conn4).

-spec remove_connection_test() -> _.
remove_connection_test() ->
    St0 = new(),
    St1 = add_connection(connection_1, St0),
    St2 = add_connection(connection_2, St1),
    St3 = add_connection(connection_3, St2),
    ?assertEqual(St3, remove_connection(connection_99, St3)),
    ?assertEqual(St3, remove_connection(connection_42, St3)).

-endif.
