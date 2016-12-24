%% @doc supervise the Riak API services

-module(riak_api_sup).

-behaviour(supervisor).

-export([start_link/0,
         init/1]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

-define(LNAME(IP, Port), lists:flatten(io_lib:format("pb://~p:~p", [IP, Port]))).
-define(TLSNAME(IP, Port), lists:flatten(io_lib:format("tls://~p:~p", [IP, Port]))).

-define(PB_LISTENER(IP, Port), {?LNAME(IP, Port),
                                {riak_api_pb_listener, start_link, [pb, IP, Port]},
                                permanent, 5000, worker, [riak_api_pb_listener]}).
-define(TLS_LISTENER(IP, Port), {?TLSNAME(IP, Port),
                                 {riak_api_pb_listener, start_link, [tls, IP, Port]},
                                 permanent, 5000, worker, [riak_api_pb_listener]}).
%% @doc Starts the supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc The init/1 supervisor callback, initializes the supervisor.
-spec init(list()) -> {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore when
      RestartStrategy :: supervisor:strategy(),
      MaxR :: pos_integer(),
      MaxT :: pos_integer(),
      ChildSpec :: supervisor:child_spec().
init([]) ->
    Helper = ?CHILD(riak_api_pb_registration_helper, worker),
    Registrar = ?CHILD(riak_api_pb_registrar, worker),
    PbListeners = riak_api_pb_listener:get_listeners(pb),
    TlsListeners = riak_api_pb_listener:get_listeners(tls),
    PBProcesses = pb_processes(PbListeners, TlsListeners),
    WebProcesses = web_processes(riak_api_web:get_listeners()),
    NetworkProcesses = PBProcesses ++ WebProcesses,
    {ok, {{one_for_one, 10, 10}, [Helper, Registrar|NetworkProcesses]}}.

%% Generates child specs from the HTTP/HTTPS listener configuration.
%% @private
web_processes([]) ->
    lager:info("No HTTP/HTTPS listeners were configured, HTTP connections will be disabled."),
    [];
web_processes(Listeners) ->
    lists:flatten([web_listener_spec(Scheme, Binding) || {Scheme, Binding} <- Listeners]).

web_listener_spec(Scheme, Binding) ->
    riak_api_web:binding_config(Scheme, Binding).

%% Generates child specs from the PB listener configuration.
%% @private
pb_processes([], []) ->
    lager:info("No PB or TLS listeners were configured, "
               "these connections will be disabled."),
    [];
pb_processes(PbListeners, TlsListeners) ->
    PbSpecs = pb_listener_specs(PbListeners),
    TlsSpecs = tls_listener_specs(TlsListeners),
    [?CHILD(riak_api_pb_sup, supervisor) | PbSpecs ++ TlsSpecs].

%% @private
pb_listener_specs(Pairs) ->
    [?PB_LISTENER(IP, Port) || {IP, Port} <- Pairs].

%% @private
tls_listener_specs(Pairs) ->
    [?TLS_LISTENER(IP, Port) || {IP, Port} <- Pairs].
