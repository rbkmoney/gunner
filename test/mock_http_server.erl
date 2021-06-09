-module(mock_http_server).

-export([start/2]).
-export([get_port/1]).
-export([start/3]).
-export([start/4]).
-export([stop/1]).

-export([init/2]).

-spec start(ranch:ref(), fun()) -> pid().
start(ID, Handler) ->
    start(ID, Handler, #{}).

-spec start(ranch:ref(), fun(), map()) -> pid().
start(ID, Handler, CowboyConf) ->
    start(ID, 0, Handler, CowboyConf).

-spec start(ranch:ref(), integer(), fun(), map()) -> pid().
start(ID, Port, Handler, CowboyConf) ->
    Dispatch = cowboy_router:compile([
        {'_', [{'_', ?MODULE, #{handler => Handler}}]}
    ]),
    {ok, Pid} = cowboy:start_clear(
        ID,
        [{port, Port}, {backlog, 8196}],
        CowboyConf#{env => #{dispatch => Dispatch}}
    ),
    Pid.

-spec get_port(ranch:ref()) -> integer().
get_port(ID) ->
    ranch:get_port(ID).

-spec stop(ranch:ref()) -> ok.
stop(ID) ->
    cowboy:stop_listener(ID).

-spec init(cowboy_req:req(), any()) -> {ok, cowboy_req:req(), any()}.
init(Req0, State = #{handler := Handler}) ->
    {Code, Headers, Body} = Handler(Req0),
    {ok, cowboy_req:reply(Code, Headers, Body, Req0), State}.
