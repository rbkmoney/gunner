-module(gunner_pool_worker_group).

%% API functions

-export([new/0]).
-export([is_empty/1]).
-export([size/1]).

-export([add_worker/2]).
-export([delete_worker/2]).

-export([next_worker/1]).

%% API Types

-type state() :: queue:queue().

-export_type([state/0]).

%% Internal types

-type worker() :: gunner_worker_factory:worker(_).

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

-spec add_worker(worker(), state()) -> state().
add_worker(Worker, GroupState) ->
    queue:in(Worker, GroupState).

-spec delete_worker(worker(), state()) -> state().
delete_worker(Worker, GroupState) ->
    queue:filter(fun(W) -> Worker =/= W end, GroupState).

-spec next_worker(state()) -> {worker(), state()}.
next_worker(GroupState0) ->
    {{value, Worker}, GroupState1} = queue:out(GroupState0),
    {Worker, GroupState1}.
