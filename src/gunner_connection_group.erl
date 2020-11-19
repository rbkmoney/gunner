%% @doc
%% The connection group is the smallest component of a connection pool. It tracks the amount
%% of connections opened to a specific endpoint (f.e. host and port pair), the "load" spread
%% between those connections (f.e. amount of times a connection has been requested from group
%% but not freed back to it), and a map of #{connection_pid() => load()}

-module(gunner_connection_group).

%% API

-export([new/0]).

-export([size/1]).
-export([load/1]).
-export([load/2]).

-export([is_member/2]).

-export([add_connection/3]).
-export([delete_connection/2]).

-export([acquire_connection/1]).
-export([free_connection/2]).

%% API Types

-type state() :: #{
    size := integer(),
    load := load(),
    connections := connections()
}.

-export_type([state/0]).

%% Private types

-type load() :: integer().
-type connections() :: #{
    connection() => load()
}.

-type connection() :: gunner_connection:connection().

%% API

-spec new() -> state().
new() ->
    #{
        size => 0,
        load => 0,
        connections => #{}
    }.

-spec size(state()) -> integer().
size(#{size := Size}) ->
    Size.

-spec load(state()) -> integer().
load(#{load := Load}) ->
    Load.

-spec load(connection(), state()) -> integer() | undefined.
load(Connection, #{connections := Connections}) ->
    maps:get(Connection, Connections, undefined).

-spec is_member(connection(), state()) -> boolean().
is_member(Connection, #{connections := Connections}) ->
    maps:is_key(Connection, Connections).

-spec add_connection(connection(), load(), state()) -> state().
add_connection(Connection, ConnLoad, St0 = #{connections := Connections, load := Load, size := Size}) ->
    St0#{
        connections => add_to_connections(Connection, ConnLoad, Connections),
        load => Load + ConnLoad,
        size => Size + 1
    }.

-spec delete_connection(connection(), state()) -> {ok, state()} | {error, connection_not_found}.
delete_connection(Connection, St0 = #{connections := Connections0, load := Load, size := Size}) ->
    case remove_from_connections(Connection, Connections0) of
        {ok, Connections1} ->
            ConnLoad = load(Connection, St0),
            {ok, St0#{connections => Connections1, load => Load - ConnLoad, size => Size - 1}};
        {error, not_found} ->
            {error, connection_not_found}
    end.

-spec acquire_connection(state()) -> {ok, connection(), state()} | {error, no_connections}.
acquire_connection(#{size := Size}) when Size =< 0 ->
    {error, no_connections};
acquire_connection(St0 = #{connections := Connections0, load := Load}) ->
    {ConnPid, ConnLoad} = maps:fold(
        fun
            (CPid, CLoad, undefined) ->
                {CPid, CLoad};
            (CPid, CLoad, {_MinPid, MinLoad}) when CLoad < MinLoad ->
                {CPid, CLoad};
            (_CPid, CLoad, {MinPid, MinLoad}) when CLoad =< MinLoad ->
                {MinPid, MinLoad}
        end,
        undefined,
        Connections0
    ),
    Connections1 = Connections0#{ConnPid => ConnLoad + 1},
    {ok, ConnPid, St0#{connections => Connections1, load => Load + 1}}.

-spec free_connection(connection(), state()) -> {ok, state()} | {error, no_load | no_connection}.
free_connection(Connection, St0 = #{connections := Connections0, load := Load}) when Load > 0 ->
    %% @TODO it's technically possible for the client to free a connection multiple times and mess up the counter,
    %% but I'm not sure what to do about it.
    case load(Connection, St0) of
        ConnectionLoad when is_integer(ConnectionLoad), ConnectionLoad > 0 ->
            Connections1 = Connections0#{Connection => ConnectionLoad - 1},
            {ok, St0#{connections => Connections1, load => Load - 1}};
        ConnectionLoad when is_integer(ConnectionLoad), ConnectionLoad =< 0 ->
            {error, no_connection_load};
        undefined ->
            {error, no_connection}
    end;
free_connection(_Connection, _St0) ->
    {error, no_load}.

%% Private

-spec add_to_connections(connection(), load(), connections()) -> connections().
add_to_connections(Connection, Load, Connections) ->
    Connections#{Connection => Load}.

-spec remove_from_connections(connection(), connections()) -> {ok, connections()} | {error, not_found}.
remove_from_connections(Connection, Connections) ->
    case maps:is_key(Connection, Connections) of
        true ->
            {ok, maps:without([Connection], Connections)};
        false ->
            {error, not_found}
    end.

%%

-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

-spec add_delete_connection_test() -> _.

add_delete_connection_test() ->
    GroupSt0 = gunner_connection_group:new(),
    GroupSt1 = gunner_connection_group:add_connection(a_pid, 0, GroupSt0),
    ?assertEqual(1, gunner_connection_group:size(GroupSt1)),
    {ok, GroupSt2} = gunner_connection_group:delete_connection(a_pid, GroupSt1),
    ?assertEqual(0, gunner_connection_group:size(GroupSt2)).

-spec delete_nonexistant_connection_test() -> _.
delete_nonexistant_connection_test() ->
    GroupSt0 = gunner_connection_group:new(),
    GroupSt1 = gunner_connection_group:add_connection(a_pid, 0, GroupSt0),
    ?assertEqual(1, gunner_connection_group:size(GroupSt1)),
    {error, connection_not_found} = gunner_connection_group:delete_connection(not_a_pid, GroupSt1).

-spec rotate_connections_test() -> _.
rotate_connections_test() ->
    GroupSt0 = gunner_connection_group:new(),
    GroupSt1 = gunner_connection_group:add_connection(pid_1, 0, GroupSt0),
    GroupSt2 = gunner_connection_group:add_connection(pid_2, 0, GroupSt1),

    {ok, pid_1, GroupSt3} = gunner_connection_group:acquire_connection(GroupSt2),
    {ok, pid_2, GroupSt4} = gunner_connection_group:acquire_connection(GroupSt3),
    {ok, pid_1, _} = gunner_connection_group:acquire_connection(GroupSt4).

-spec load_test() -> _.
load_test() ->
    GroupSt0 = gunner_connection_group:new(),
    GroupSt1 = gunner_connection_group:add_connection(pid_1, 0, GroupSt0),
    GroupSt2 = gunner_connection_group:add_connection(pid_2, 0, GroupSt1),

    {ok, pid_1, GroupSt3} = gunner_connection_group:acquire_connection(GroupSt2),
    ?assertEqual(1, gunner_connection_group:load(GroupSt3)),

    {ok, pid_2, GroupSt4} = gunner_connection_group:acquire_connection(GroupSt3),
    {ok, pid_1, GroupSt5} = gunner_connection_group:acquire_connection(GroupSt4),
    ?assertEqual(3, gunner_connection_group:load(GroupSt5)),

    {ok, GroupSt6} = gunner_connection_group:free_connection(pid_1, GroupSt5),
    ?assertEqual(2, gunner_connection_group:load(GroupSt6)),

    {ok, GroupSt7} = gunner_connection_group:free_connection(pid_2, GroupSt6),
    {ok, GroupSt8} = gunner_connection_group:free_connection(pid_1, GroupSt7),
    ?assertEqual(0, gunner_connection_group:load(GroupSt8)).

-spec free_nonexistant_connection_test() -> _.
free_nonexistant_connection_test() ->
    GroupSt0 = gunner_connection_group:new(),
    GroupSt1 = gunner_connection_group:add_connection(pid_1, 0, GroupSt0),
    {ok, pid_1, GroupSt2} = gunner_connection_group:acquire_connection(GroupSt1),
    ?assertEqual({error, no_connection}, gunner_connection_group:free_connection(pid_2, GroupSt2)).

-spec free_no_load_connection_test() -> _.
free_no_load_connection_test() ->
    GroupSt0 = gunner_connection_group:new(),
    GroupSt1 = gunner_connection_group:add_connection(pid_1, 0, GroupSt0),
    {ok, pid_1, GroupSt2} = gunner_connection_group:acquire_connection(GroupSt1),
    GroupSt3 = gunner_connection_group:free_connection(pid_1, GroupSt2),
    ?assertEqual({error, no_load}, gunner_connection_group:free_connection(pid_1, GroupSt3)).

-endif.
