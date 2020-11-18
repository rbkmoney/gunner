-module(mock_http_server).

-export([start/2]).
-export([stop/0]).

-export([init/2]).

-define(SERVER, mock_http_server).

-spec start(integer(), fun()) -> pid.
start(Port, Handler) ->
    Dispatch = cowboy_router:compile([
        {'_', [{"/", ?MODULE, #{handler => Handler}}]}
    ]),
    {ok, Pid} = cowboy:start_clear(
        ?SERVER,
        [{port, Port}],
        #{env => #{dispatch => Dispatch}}
    ),
    Pid.

-spec stop() -> ok.
stop() ->
    cowboy:stop_listener(?SERVER).

-spec init(cowboy_req:req(), any()) -> {ok, cowboy_req:req(), any()}.
init(Req0, State = #{handler := Handler}) ->
    {Code, Headers, Body} = Handler(),
    {ok, cowboy_req:reply(Code, Headers, Body, Req0), State}.
