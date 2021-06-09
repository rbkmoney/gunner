-module(gunner_test_event_h).

%% Event Handler

-export([make_event_h/1]).

-behaviour(gunner_event_h).

-export([handle_event/2]).

-type event_h_state() :: #{server => pid()}.

%% Event Storage

-export([start_storage/0]).
-export([stop_storage/1]).
-export([push_event/2]).
-export([pop_events/1]).

-behaviour(gen_server).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).

-type storage_state() :: #{
    events => [gunner_event_h:event()]
}.

%%
%% Event Handler
%%

-spec make_event_h(pid()) -> gunner_event_h:handler().
make_event_h(Server) ->
    {?MODULE, #{server => Server}}.

-spec handle_event(gunner_event_h:event(), event_h_state()) -> event_h_state().
handle_event(Event, State = #{server := Server}) ->
    ok = push_event(Server, Event),
    State.

%%
%% Event Storage
%%

-spec start_storage() -> {ok, pid()}.
start_storage() ->
    gen_server:start(?MODULE, [], []).

-spec stop_storage(pid()) -> ok.
stop_storage(Server) ->
    gen_server:stop(Server).

-spec push_event(pid(), gunner_event_h:event()) -> ok.
push_event(Server, Event) ->
    gen_server:cast(Server, {push_event, Event}).

-spec pop_events(pid()) -> [gunner_event_h:event()].
pop_events(Server) ->
    gen_server:call(Server, pop_events).

-spec init(any()) -> {ok, storage_state()}.
init(_Args) ->
    {ok, #{events => []}}.

-spec handle_call(pop_events, _From, storage_state()) -> {reply, {ok, [gunner_event_h:event()]}, storage_state()}.
handle_call(pop_events, _From, State = #{events := Events}) ->
    {reply, {ok, lists:reverse(Events)}, State#{events => []}}.

-spec handle_cast({push_event, gunner_event_h:event()}, storage_state()) -> {noreply, storage_state()}.
handle_cast({push_event, Event}, State = #{events := Events}) ->
    {noreply, State#{events => [Event | Events]}}.
