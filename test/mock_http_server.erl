-module(mock_http_server).

-export([start/3]).
-export([start/4]).
-export([stop/1]).

-export([init/2]).

-define(SERVER, mock_http_server).

-spec start(atom(), integer(), fun()) -> pid().
start(ID, Port, Handler) ->
    start(ID, Port, Handler, #{}).

-spec start(atom(), integer(), fun(), map()) -> pid().
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

-spec stop(atom()) -> ok.
stop(ID) ->
    cowboy:stop_listener(ID).

-spec init(cowboy_req:req(), any()) -> {ok, cowboy_req:req(), any()}.
init(Req0, State = #{handler := Handler}) ->
    {Code, Headers, Body} = Handler(Req0),
    {ok, cowboy_req:reply(Code, Headers, Body, Req0), State}.
