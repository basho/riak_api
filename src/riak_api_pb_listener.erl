%% @doc entry point for TCP-based protocol buffers service

-module(riak_api_pb_listener).
-behaviour(gen_nb_server).
-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([sock_opts/0, new_connection/2]).
-export([get_listeners/1]).
-record(state, {type, portnum}).

%% @doc Starts the PB listener
-spec start_link(pb | tls, inet:ip_address() | string(),  non_neg_integer()) -> {ok, pid()} | {error, term()}.
start_link(Type, IpAddr, PortNum) ->
    gen_nb_server:start_link(?MODULE, IpAddr, PortNum, [Type, PortNum]).

%% @doc Initialization callback for gen_nb_server.
-spec init(list()) -> {ok, #state{}}.
init([Type, PortNum]) ->
    {ok, #state{type=Type,portnum=PortNum}}.

%% @doc Preferred socket options for the listener.
-spec sock_opts() -> [gen_tcp:option()].
sock_opts() ->
    BackLog = app_helper:get_env(riak_api, pb_backlog, 128),
    NoDelay = app_helper:get_env(riak_api, disable_pb_nagle, true),
    KeepAlive = app_helper:get_env(riak_api, pb_keepalive, true),
    [binary, {packet, raw}, {reuseaddr, true}, {backlog, BackLog}, {nodelay, NoDelay}, {keepalive, KeepAlive}].

%% @doc The handle_call/3 gen_nb_server callback. Unused.
-spec handle_call(term(), {pid(),_}, #state{}) -> {reply, term(), #state{}}.
handle_call(_Req, _From, State) ->
    {reply, not_implemented, State}.

%% @doc The handle_cast/2 gen_nb_server callback. Unused.
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_Msg, State) -> {noreply, State}.

%% @doc The handle_info/2 gen_nb_server callback. Unused.
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(_Info, State) -> {noreply, State}.

%% @doc The code_change/3 gen_nb_server callback. Unused.
-spec terminate(Reason, State) -> ok when
      Reason :: normal | shutdown | {shutdown,term()} | term(),
      State :: #state{}.
terminate(_Reason, _State) ->
    ok.

%% @doc The gen_server code_change/3 callback, called when performing
%% a hot code upgrade on the server. Currently unused.
-spec code_change(OldVsn, State, Extra) -> {ok, State} | {error, Reason}
                                               when
      OldVsn :: Vsn | {down, Vsn},
      Vsn :: term(),
      State :: #state{},
      Extra :: term(),
      Reason :: term().
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @doc The connection initiation callback for gen_nb_server, called
%% when a new socket is accepted.
-spec new_connection(port(), #state{}) -> {ok, #state{}}.
new_connection(Socket, #state{type=Type}=State) ->
    {ok, Pid} = riak_api_pb_sup:start_socket(),
    ok = gen_tcp:controlling_process(Socket, Pid),
    ok = riak_api_pb_server:set_socket(Pid, Socket, Type),
    {ok, State}.

get_listeners(Type) ->
    L = app_helper:get_env(riak_api, Type, []),
    [{I, P} || {I, P} <- L].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

listeners_test_() ->
    {foreach,
     fun() ->
             application:load(riak_api),
             app_helper:get_env(riak_api, pb, [{"127.0.0.1", 8087}])
     end,
     [
      {"bad configs are ignored",
       fun() ->
              application:set_env(riak_api, pb, [{"0.0.0.0", 8087}, badjuju]),
               ?assertEqual([{"0.0.0.0", 8087}], get_listeners(pb))
       end}]}.

-endif.
