-module(gunner_pool_connection_group).

%% API functions

-export([new/0]).
-export([is_empty/1]).
-export([size/1]).

-export([add_connection/2]).
-export([delete_connection/2]).

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

-spec delete_connection(connection(), state()) -> state().
delete_connection(Connection, GroupState) ->
    queue:filter(fun(W) -> Connection =/= W end, GroupState).

-spec next_connection(state()) -> {connection(), state()}.
next_connection(GroupState0) ->
    {{value, Connection}, GroupState1} = queue:out(GroupState0),
    {Connection, GroupState1}.
