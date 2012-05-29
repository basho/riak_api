%% -------------------------------------------------------------------
%%
%% riak_api_pb_listener: Listen for protocol buffer clients
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc entry point for TCP-based protocol buffers service

-module(riak_api_pb_listener).
-behaviour(gen_nb_server).
-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([sock_opts/0, new_connection/2]).
-export([get_port/0, get_ip/0]).
-record(state, {portnum}).

%% @doc Starts the PB listener
-spec start_link(inet:ip_address() | string(),  non_neg_integer()) -> {ok, pid()} | {error, term()}.
start_link(IpAddr, PortNum) ->
    gen_nb_server:start_link(?MODULE, IpAddr, PortNum, [PortNum]).

%% @doc Initialization callback for gen_nb_server.
-spec init(list()) -> {ok, #state{}}.
init([PortNum]) ->
    {ok, #state{portnum=PortNum}}.

%% @doc Preferred socket options for the listener.
-spec sock_opts() -> [gen_tcp:option()].
sock_opts() ->
    BackLog = app_helper:get_env(riak_api, pb_backlog, 5),
    NoDelay = app_helper:get_env(riak_api, disable_pb_nagle, true),
    [binary, {packet, 4}, {reuseaddr, true}, {backlog, BackLog}, {nodelay, NoDelay}].

%% @doc The handle_call/3 gen_nb_server callback. Unused.
-spec handle_call(term(), pid(), #state{}) -> {reply, term(), #state{}}.
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
-spec new_connection(gen_tcp:socket(), #state{}) -> {ok, #state{}}.
new_connection(Socket, State) ->
    {ok, Pid} = riak_api_pb_sup:start_socket(),
    ok = gen_tcp:controlling_process(Socket, Pid),
    ok = riak_api_pb_server:set_socket(Pid, Socket),
    {ok, State}.

%% @private
get_port() ->
    Envs = [{riak_api, pb_port},
            {riak_kv, pb_port}],
    case app_helper:try_envs(Envs) of
        {riak_api, pb_port, Port} ->
            Port;
        {riak_kv, pb_port, Port} ->
            lager:warning("The config riak_kv/pb_port has been"
                          " deprecated and will be removed.  Use"
                          " riak_api/pb_port in the future."),
            Port;
        {default, undefined} ->
            lager:warning("The config riak_api/pb_port is missing,"
                          " PB connections will be disabled."),
            undefined
    end.

%% @private
get_ip() ->
    Envs = [{riak_api, pb_ip},
            {riak_kv, pb_ip}],
    case app_helper:try_envs(Envs) of
        {riak_api, pb_ip, IP} ->
            IP;
        {riak_kv, pb_ip, IP} ->
            lager:warning("The config riak_kv/pb_ip has been"
                          " deprecated and will be removed.  Use"
                          " riak_api/pb_ip in the future."),
            IP;
        {default, undefined} ->
            lager:warning("The config riak_api/pb_ip is missing,"
                          " PB connections will be disabled."),
            undefined
    end.
