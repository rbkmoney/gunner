-module(gunner_idx_authority).

%% API functions

-export([new/1]).
-export([get_index/1]).
-export([free_index/2]).

%% API Types

-type idx() :: non_neg_integer().
-type t() :: queue:queue().

%% Internal types

-type size() :: non_neg_integer().

%%
%% API functions
%%

-spec new(size()) -> t().
new(Size) ->
    queue:from_list(lists:seq(1, Size)).

-spec get_index(t()) -> {ok, idx(), t()} | {error, no_free_indices}.
get_index(St) ->
    case queue:out(St) of
        {{value, Idx}, NewSt} ->
            {ok, Idx, NewSt};
        {empty, St} ->
            {error, no_free_indices}
    end.

-spec free_index(idx(), t()) -> {ok, t()}.
free_index(Idx, St) ->
    {ok, queue:in(Idx, St)}.
