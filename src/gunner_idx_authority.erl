-module(gunner_idx_authority).

%% API functions

-export([new/1]).
-export([get_index/1]).
-export([free_index/2]).

%% API Types

-type idx() :: non_neg_integer().
-type t() :: list(integer()).

-export_type([idx/0]).
-export_type([t/0]).

%% Internal types

-type size() :: non_neg_integer().

%%
%% API functions
%%

-spec new(size()) -> t().
new(Size) ->
    lists:seq(1, Size).

-spec get_index(t()) -> {ok, idx(), t()} | {error, no_free_indices}.
get_index([]) ->
    {error, no_free_indices};
get_index([Idx | Rest]) ->
    {ok, Idx, Rest}.

-spec free_index(idx(), t()) -> {ok, t()}.
free_index(Idx, St) ->
    {ok, [Idx | St]}.
