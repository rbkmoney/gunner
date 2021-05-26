-module(gunner_test_event_h).

-type event_h_id() :: atom().

%% Event Handler

-export([make_event_h/2]).

-behaviour(gunner_event_h).

-export([handle_event/2]).

-type event_h_state() :: #{server => pid()}.

%% Event Storage

-export([start_storage/0]).
-export([stop_storage/1]).
-export([push_event/3]).
-export([pop_events/2]).

-behaviour(gen_server).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).

-type storage_state() :: #{
    events => #{event_h_id() => [gunner_event_h:event()]}
}.

%%
%% Event Handler
%%

-spec make_event_h(event_h_id(), pid()) -> gunner_event_h:handler().
make_event_h(Id, Server) ->
    {?MODULE, #{id => Id, server => Server}}.

-spec handle_event(gunner_event_h:event(), event_h_state()) -> event_h_state().
handle_event(Event, State = #{id := Id, server := Server}) ->
    ok = push_event(Server, Id, Event),
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

-spec push_event(pid(), event_h_id(), gunner_event_h:event()) -> ok.
push_event(Server, Id, Event) ->
    gen_server:cast(Server, {push_event, Id, Event}).

-spec pop_events(pid(), event_h_id()) -> [gunner_event_h:event()].
pop_events(Server, Id) ->
    gen_server:call(Server, {pop_events, Id}).

-spec init(any()) -> {ok, storage_state()}.
init(_Args) ->
    {ok, #{events => #{}}}.

-spec handle_call({pop_events, event_h_id()}, _From, storage_state()) ->
    {reply, {ok, [gunner_event_h:event()]}, storage_state()}.
handle_call({pop_events, Id}, _From, State = #{events := HandlerEvents}) ->
    Events = maps:get(Id, HandlerEvents, []),
    {reply, {ok, lists:reverse(Events)}, State#{events => HandlerEvents#{Id => []}}}.

-spec handle_cast({push_event, gunner_event_h:event()}, storage_state()) -> {noreply, storage_state()}.
handle_cast({push_event, Id, Event}, State = #{events := HandlerEvents}) ->
    Events = maps:get(Id, HandlerEvents, []),
    {noreply, State#{events => HandlerEvents#{Id => [Event | Events]}}}.
