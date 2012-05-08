%% -------------------------------------------------------------------
%%
%% riak_kv_pb_socket: service protocol buffer clients
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Service protocol buffer clients. This module implements only
%% the TCP socket management and dispatch of incoming messages to
%% service modules.

-module(riak_api_pb_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-export([start_link/0, set_socket/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          socket :: port(),   % socket
          req,                % current request
          dispatch :: dict(), % dispatch table of msg code -> service
          states :: dict()    % per-service connection state
         }).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Starts a PB server, ready to service a single socket.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%% @doc Sets the socket to service for this server.
-spec set_socket(pid(), port()) -> ok.
set_socket(Pid, Socket) ->
    gen_server:call(Pid, {set_socket, Socket}, infinity).

%% @doc The gen_server init/1 callback, initializes the
%% riak_api_pb_server.
-spec init(list()) -> {ok, #state{}}.
init([]) ->
    %% TODO: use Russell's new stats system
    %% riak_kv_stat:update(pbc_connect)
    Dispatch = riak_api_pb_service:dispatch_table(),
    ServiceStates = lists:foldl(fun(Service, States) ->
                                        dict:store(Service, Service:init(), States)
                                end,
                                dict:new(), riak_api_pb_service:services()),
    {ok, #state{dispatch=Dispatch,
                states=ServiceStates}}.

%% @doc The handle_call/3 gen_server callback.
-spec handle_call(Message::term(), From::{pid(),term()}, State::#state{}) -> {reply, Message::term(), NewState::#state{}}.
handle_call({set_socket, Socket}, _From, State) ->
    inet:setopts(Socket, [{active, once}, {packet, 4}, {header, 1}]),
    {reply, ok, State#state{socket = Socket}}.

%% @doc The handle_cast/2 gen_server callback.
-spec handle_cast(Message::term(), State::#state{}) -> {noreply, NewState::#state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc The handle_info/2 gen_server callback.
-spec handle_info(Message::term(), State::#state{}) -> {noreply, NewState::#state{}} | {stop, Reason::atom(), NewState::#state{}}.
handle_info({Ref, done}, #state{req={_Service,Ref}}=State) ->
    %% Streaming requests set the req reference.
    {noreply, State#state{req=undefined}};
handle_info({Ref, {MessageCode, Message}}, #state{req={Service,Ref}}=State) ->
    {ok, Encoded} = Service:encode(MessageCode, Message),
    send_message(Encoded, State),
    {noreply, State};
handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, _Reason}, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp, _Sock, [MsgCode|MsgData]}, State=#state{
                                               socket=Socket,
                                               req=undefined,
                                               dispatch=Dispatch,
                                               states=ServiceStates}) ->
    %% First find the appropriate service module to dispatch
    case dict:find(MsgCode, Dispatch) of
        {ok, Service} ->
            %% Decode the message according to the service
            case Service:decode(MsgCode, MsgData) of
                {ok, Message} ->
                    %% Process the message
                    ServiceState = dict:fetch(Service, ServiceStates),
                    NewState = process_message(Service, Message, ServiceState, State);
                {error, Reason} ->
                    send_error("Message decoding error: ~p", [Reason], State),
                    NewState = State
            end;
        error ->
            send_error("Unknown message code.", State),
            NewState = State
    end,
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState};
handle_info({tcp, _Sock, _Data}, State) ->
    %% req =/= undefined: received a new request while another was in
    %% progress -> Error
    lager:error("Received a new PB socket request"
                " while another was in progress"),
    send_error("Cannot send another request while one is in progress", State),
    {stop, normal, State}.


%% @doc The gen_server terminate/2 callback, called when shutting down
%% the server.
-spec terminate(Reason, State) -> ok when
      Reason :: normal | shutdown | {shutdown,term()} | term(),
      State :: #state{}.
terminate(_Reason, _State) ->
    %% TODO: Update with Russell's new stats system
    %% riak_kv_stat:update(pbc_disconnect),
    ok.

%% @doc The gen_server code_change/3 callback, called when performing
%% a hot code upgrade on the server. Currently unused.
-spec code_change(OldVsn, State, Extra) -> {ok, State} | {error, Reason} when
      OldVsn :: Vsn | {down, Vsn},
      Vsn :: term(),
      State :: #state{},
      Extra :: term(),
      Reason :: term().
code_change(_OldVsn,State,_Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Dispatches an incoming message to the registered service that
%% recognizes it. This is called after the message has been identified
%% and decoded.
-spec process_message(atom(), term(), term(), #state{}) -> #state{}.
process_message(Service, Message, ServiceState, #state{states=ServiceStates}=ServerState) ->
    case Service:process(Message, ServiceState) of
        %% Streaming reply with reference
        {reply, {stream, Ref}, NewServiceState} ->
            NewServiceStates = dict:store(Service, NewServiceState, ServiceStates),
            ServerState#state{states=NewServiceStates, req={Service,Ref}};
        %% Normal reply
        {reply, ReplyMessage, NewServiceState} ->
            {ok, Encoded} = Service:encode(ReplyMessage),
            send_message(Encoded, ServerState),
            NewServiceStates = dict:store(Service, NewServiceState, ServiceStates),
            ServerState#state{states=NewServiceStates};
        {error, Message, NewServiceState} ->
            send_error(Message, ServerState),
            NewServiceStates = dict:store(Service, NewServiceState, ServiceStates),
            ServerState#state{states=NewServiceStates};
        Other ->
            send_error("Unknown service response: ~p", [Other], ServerState),
            ServerState
    end.


%% @doc Sends a regular message to the client
-spec send_message(binary(), #state{}) -> ok | {error, term()}.
send_message(Bin, #state{socket=Sock}) ->
    gen_tcp:send(Sock, Bin).

%% @doc Sends an error message to the client
-spec send_error(iolist(), #state{}) -> ok | {error, term()}.
send_error(Message, State) ->
    %% TODO: provide a service for encoding error messages? While
    %% extra work, it would follow the pattern. On the other hand,
    %% maybe it's too much abstraction. This is a hack, allowing us
    %% to avoid including the header file.
    Packet = riakc_pb:encode({rpberrorresp,Message,0}),
    send_message(Packet, State).

%% @doc Formats the terms with the given string and then sends an
%% error message to the client.
-spec send_error(io:format(), list(), #state{}) -> ok | {error, term()}.
send_error(Format, Terms, State) ->
    send_error(io_lib:format(Format, Terms), State).
