%% @doc supervise riak_api_pb_server processes

-module(riak_api_pb_sup).
-behaviour(supervisor).
-export([start_link/0, init/1, stop/1]).
-export([start_socket/0, service_registered/1]).

%% @doc Starts a PB socket server.
-spec start_socket() -> {ok, pid()} | {error, term()}.
start_socket() ->
    supervisor:start_child(?MODULE, []).

%% @doc Notifies connected client sockets of a new service so they can
%% initialize it. Called internally by `riak_api_pb_service:register/3'.
-spec service_registered(module()) -> ok.
service_registered(Mod) ->
    case erlang:whereis(?MODULE) of
        undefined ->
            ok;
        _ ->
            _ = [ riak_api_pb_server:service_registered(Pid, Mod) ||
                    {_,Pid,_,_} <- supervisor:which_children(?MODULE) ],
            ok
    end.

%% @doc Starts the PB server supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Stops the PB server supervisor.
-spec stop(term()) -> ok.
stop(_S) -> ok.

%% @doc The init/1 supervisor callback, initializes the supervisor.
-spec init(list()) -> {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore when
      RestartStrategy :: supervisor:strategy(),
      MaxR :: pos_integer(),
      MaxT :: pos_integer(),
      ChildSpec :: supervisor:child_spec().
init([]) ->
    {ok,
     {{simple_one_for_one, 10, 10},
      [{undefined,
        {riak_api_pb_server, start_link, []},
        temporary, brutal_kill, worker, [riak_api_pb_server]}]}}.
