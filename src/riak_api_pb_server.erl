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
handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, _Reason}, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp, _Sock, [MsgCode|MsgData]}, State=#state{
                                               socket=Socket,
                                               req=undefined,
                                               dispatch=Dispatch,
                                               states=ServiceStates}) ->
    try
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
        {noreply, NewState}
    catch
        %% Tell the client we errored before closing the connection.
        Type:Failure ->
            lager:error("Message processing error: ~p", [Failure]),
            send_error("Error processing incoming message: ~p:~p", [Type, Failure], State),
            {stop, {Type, Failure}, State}
    end;
handle_info({tcp, _Sock, _Data}, State) ->
    %% req =/= undefined: received a new request while another was in
    %% progress -> Error
    lager:error("Received a new PB socket request"
                " while another was in progress"),
    send_error("Cannot send another request while one is in progress", State),
    {stop, normal, State};
handle_info(StreamMessage, #state{req={Service,ReqId},
                                  states=ServiceStates}=State) ->
    %% Handle streaming messages from other processes. This should
    %% help avoid creating extra middlemen. Naturally, this is only
    %% valid when a streaming request has started, other messages will
    %% be ignored.
    try
        ServiceState = dict:fetch(Service, ServiceStates),
        NewState = process_stream(Service, ReqId, StreamMessage, ServiceState, State),
        {noreply, NewState}
    catch
        %% Tell the client we errored before closing the connection.
        Type:Reason ->
            lager:error("Streaming message processing error (State: ~p): ~p", [State, Reason]),
            send_error("Error processing stream message: ~p:~p", [Type, Reason], State),
            {stop, {Type, Reason}, State}
    end;
handle_info(Message, State) ->
    %% Throw out messages we don't care about, but log them at the
    %% debug level.
    lager:debug("Unrecognized message ~p", [Message]),
    {noreply, State}.


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
process_message(Service, Message, ServiceState, ServerState) ->
    case Service:process(Message, ServiceState) of
        %% Streaming reply with reference
        {reply, {stream, ReqId}, NewServiceState} ->
            update_service_state(Service, NewServiceState, ServerState#state{req={Service,ReqId}});
        %% Normal reply
        {reply, ReplyMessage, NewServiceState} ->
            send_encoded_message_or_error(Service, ReplyMessage, ServerState),
            update_service_state(Service, NewServiceState, ServerState);
        %% Recoverable error
        {error, Message, NewServiceState} ->
            send_error(Message, ServerState),
            update_service_state(Service, NewServiceState, ServerState);
        %% Result is broken
        Other ->
            send_error("Unknown PB service response: ~p", [Other], ServerState),
            ServerState
    end.

%% @doc Processes a message received from a stream. These are received
%% on the server process so that we can avoid middlemen, but need to
%% be translated into responses according to the service producing
%% them.
-spec process_stream(module(), term(), term(), term(), #state{}) -> #state{}.
process_stream(Service, ReqId, Message, ServiceState0, State) ->
    case Service:process_stream(Message, ReqId, ServiceState0) of
        %% Give the service the opportunity to throw out messages it
        %% doesn't care about.
        {ignore, ServiceState} ->
            update_service_state(Service, ServiceState, State);
        %% Regular middle-of-stream messages
        {reply, Reply, ServiceState} ->
            send_encoded_message_or_error(Service, Reply, State),
            update_service_state(Service, ServiceState, State);
        %% Stop the stream with a final reply
        {done, Reply, ServiceState} ->
            send_encoded_message_or_error(Service, Reply, State),
            update_service_state(Service, ServiceState, State);
        %% Stop the stream without sending a client reply
        {done, ServiceState} ->
            update_service_state(Service, ServiceState, State#state{req=undefined});
        %% Send the client normal errors
        {error, Reason, ServiceState} ->
            send_error(Reason, State),
            update_service_state(Service, ServiceState, State#state{req=undefined});
        Other ->
            send_error("Unknown PB service response: ~p", [Other], State),
            State
    end.

%% @doc Updates the given service state and puts it in the server's state.
-spec update_service_state(module(), term(), #state{}) -> #state{}.
update_service_state(Service, NewServiceState, #state{states=ServiceStates}=ServerState) ->
    NewServiceStates = dict:store(Service, NewServiceState, ServiceStates),
    ServerState#state{states=NewServiceStates}.

%% @doc Given an unencoded response message, attempts to encode it and send it
%% to the client.
-spec send_encoded_message_or_error(module(), term(), #state{}) -> any().
send_encoded_message_or_error(Service, ReplyMessage, ServerState) ->
    case Service:encode(ReplyMessage) of
        {ok, Encoded} ->
            send_message(Encoded, ServerState);
        Error ->
            lager:error("PB service ~p could not encode message ~p: ~p", [Service, ReplyMessage, Error]),
            send_error("Internal service error: no encoding for response message", ServerState)
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
