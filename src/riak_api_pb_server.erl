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
          states :: orddict:orddict(),    % per-service connection state
          buffer = riak_api_pb_frame:new() :: riak_api_pb_frame:buffer() % frame buffer which we can use to optimize TCP sends
         }).

-type format() :: {format, term()} | {format, io:format(), [term()]}.
-export_type([format/0]).

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
    riak_api_stat:update(pbc_connect),
    ServiceStates = lists:foldl(fun(Service, States) ->
                                        orddict:store(Service, Service:init(), States)
                                end,
                                orddict:new(), riak_api_pb_registrar:services()),
    {ok, #state{states=ServiceStates}}.

%% @doc The handle_call/3 gen_server callback.
-spec handle_call(Message::term(), From::{pid(),term()}, State::#state{}) -> {reply, Message::term(), NewState::#state{}}.
handle_call({set_socket, Socket}, _From, State) ->
    inet:setopts(Socket, [{active, once}, {header, 1}]),
    {reply, ok, State#state{socket = Socket}}.

%% @doc The handle_cast/2 gen_server callback.
-spec handle_cast(Message::term(), State::#state{}) -> {noreply, NewState::#state{}, timeout()}.
handle_cast({registered, Service}, #state{states=ServiceStates}=State) ->
    %% When a new service is registered after a client connection is
    %% already established, update the internal state to support the
    %% new capabilities.
    case orddict:is_key(Service, ServiceStates) of
        true ->
            %% This is an existing service registering
            %% disjoint message codes
            {noreply, State, 0};
        false ->
            %% This is a new service registering
            {noreply, State#state{states=orddict:store(Service, Service:init(), ServiceStates)}, 0}
    end;
handle_cast(_Msg, State) ->
    {noreply, State, 0}.

%% @doc The handle_info/2 gen_server callback.
-spec handle_info(Message::term(), State::#state{}) -> {noreply, NewState::#state{}} | {stop, Reason::atom(), NewState::#state{}}.
handle_info(timeout, #state{buffer=Buffer}=State) ->
    %% Flush any protocol messages that have been buffering
    {ok, Data, NewBuffer} = riak_api_pb_frame:flush(Buffer),
    {noreply, flush(Data, State#state{buffer=NewBuffer})};
handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, _Reason}, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp, _Sock, [MsgCode|MsgData]}, State=#state{
                                               socket=Socket,
                                               req=undefined,
                                               states=ServiceStates}) ->
    try
        %% First find the appropriate service module to dispatch
        NewState = case riak_api_pb_registrar:lookup(MsgCode) of
            {ok, Service} ->
                %% Decode the message according to the service
                case Service:decode(MsgCode, MsgData) of
                    {ok, Message} ->
                        %% Process the message
                        ServiceState = orddict:fetch(Service, ServiceStates),
                        process_message(Service, Message, ServiceState, State);
                    {error, Reason} ->
                        send_error("Message decoding error: ~p", [Reason], State)
                end;
            error ->
                send_error("Unknown message code.", State)
        end,
        inet:setopts(Socket, [{active, once}]),
        {noreply, NewState, 0}
    catch
        %% Tell the client we errored before closing the connection.
        Type:Failure ->
            Trace = erlang:get_stacktrace(),
            FState = send_error_and_flush({format, "Error processing incoming message: ~p:~p:~p",
                                           [Type, Failure, Trace]}, State),
            {stop, {Type, Failure, Trace}, FState}
    end;
handle_info({tcp, _Sock, _Data}, State) ->
    %% req =/= undefined: received a new request while another was in
    %% progress -> Error
    lager:debug("Received a new PB socket request"
                " while another was in progress"),
    State1 = send_error_and_flush("Cannot send another request while one is in progress", State),
    {stop, normal, State1};
handle_info(StreamMessage, #state{req={Service,ReqId,StreamState}}=State) ->
    %% Handle streaming messages from other processes. This should
    %% help avoid creating extra middlemen. Naturally, this is only
    %% valid when a streaming request has started, other messages will
    %% be ignored.
    try
        NewState = process_stream(Service, ReqId, StreamMessage, StreamState, State),
        {noreply, NewState, 0}
    catch
        %% Tell the client we errored before closing the connection.
        Type:Reason ->
            Trace = erlang:get_stacktrace(),
            FState = send_error_and_flush({format, "Error processing stream message: ~p:~p:~p",
                                          [Type, Reason, Trace]}, State),
            {stop, {Type, Reason, Trace}, FState}
    end;
handle_info(Message, State) ->
    %% Throw out messages we don't care about, but log them
    lager:error("Unrecognized message ~p", [Message]),
    {noreply, State, 0}.


%% @doc The gen_server terminate/2 callback, called when shutting down
%% the server.
-spec terminate(Reason, State) -> ok when
      Reason :: normal | shutdown | {shutdown,term()} | term(),
      State :: #state{}.
terminate(_Reason, _State) ->
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
            update_service_state(Service, NewServiceState, ServiceState, ServerState#state{req={Service,ReqId,NewServiceState}});
        %% Normal reply
        {reply, ReplyMessage, NewServiceState} ->
            ServerState1 = send_encoded_message_or_error(Service, ReplyMessage, ServerState),
            update_service_state(Service, NewServiceState, ServiceState, ServerState1);
        %% Recoverable error
        {error, ErrorMessage, NewServiceState} ->
            ServerState1 = send_error(ErrorMessage, ServerState),
            update_service_state(Service, NewServiceState, ServiceState, ServerState1);
        %% Result is broken
        Other ->
            send_error("Unknown PB service response: ~p", [Other], ServerState)
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
            update_service_state(Service, ServiceState, ServiceState0, State);
        %% Sending multiple replies in middle-of-stream
        {reply, Replies, ServiceState} when is_list(Replies) ->
            State1 = send_all(Service, Replies, State),
            update_service_state(Service, ServiceState, ServiceState0, State1);
        %% Regular middle-of-stream messages
        {reply, Reply, ServiceState} ->
            State1 = send_encoded_message_or_error(Service, Reply, State),
            update_service_state(Service, ServiceState, ServiceState0, State1);
        %% Stop the stream with multiple final replies
        {done, Replies, ServiceState} when is_list(Replies) ->
            State1 = send_all(Service, Replies, State),
            update_service_state(Service, ServiceState, ServiceState0, State1#state{req=undefined});
        %% Stop the stream with a final reply
        {done, Reply, ServiceState} ->
            State1 = send_encoded_message_or_error(Service, Reply, State),
            update_service_state(Service, ServiceState, ServiceState0, State1#state{req=undefined});
        %% Stop the stream without sending a client reply
        {done, ServiceState} ->
            update_service_state(Service, ServiceState, ServiceState0, State#state{req=undefined});
        %% Send the client normal errors
        {error, Reason, ServiceState} ->
            State1 = send_error(Reason, State),
            update_service_state(Service, ServiceState, ServiceState0, State1#state{req=undefined});
        Other ->
            send_error("Unknown PB service response: ~p", [Other], State)
    end.

%% @doc Updates the given service state and puts it in the server's state.
-spec update_service_state(module(), term(), term(), #state{}) -> #state{}.
update_service_state(Service, NewServiceState, _OldServiceState, #state{req={Service,ReqId,_StreamState}}=ServerState) ->
    %% While streaming, we avoid extra fetches of the state by
    %% including it in the current request field. When req is
    %% undefined (set at the end of the stream), it will be updated
    %% into the orddict.
    ServerState#state{req={Service,ReqId,NewServiceState}};
update_service_state(_Service, OldServiceState, OldServiceState, ServerState) ->
    %% If the service state is unchanged, don't bother storing it again.
    ServerState;
update_service_state(Service, NewServiceState, _OldServiceState, #state{states=ServiceStates}=ServerState) ->
    NewServiceStates = orddict:store(Service, NewServiceState, ServiceStates),
    ServerState#state{states=NewServiceStates}.

%% @doc Given an unencoded response message, attempts to encode it and send it
%% to the client.
-spec send_encoded_message_or_error(module(), term(), #state{}) -> #state{}.
send_encoded_message_or_error(Service, ReplyMessage, ServerState) ->
    case Service:encode(ReplyMessage) of
        {ok, Encoded} ->
            send_message(Encoded, ServerState);
        Error ->
            lager:error("PB service ~p could not encode message ~p: ~p",
                        [Service, ReplyMessage, Error]),
            send_error("Internal service error: no encoding for response message", ServerState)
    end.

%% @doc Sends a regular message to the client
-spec send_message(iodata(), #state{}) -> #state{}.
send_message(Bin, #state{buffer=Buffer}=State) when is_binary(Bin) orelse is_list(Bin) ->
    case riak_api_pb_frame:add(Bin, Buffer) of
        {ok, Buffer1} ->
            State#state{buffer=Buffer1};
        {flush, IoData, Buffer1} ->
            flush(IoData, State#state{buffer=Buffer1})
    end.


%% @doc Sends an error message to the client
-spec send_error(iolist() | format(), #state{}) -> #state{}.
send_error({format, Term}, State) ->
    send_error({format, "~p", [Term]}, State);
send_error({format, Fmt, TList}, State) ->
    send_error(io_lib:format(Fmt, TList), State);
send_error(Message, State) when is_list(Message) orelse is_binary(Message) ->
    %% TODO: provide a service for encoding error messages? While
    %% extra work, it would follow the pattern. On the other hand,
    %% maybe it's too much abstraction. This is a hack, allowing us
    %% to avoid including the header file.
    Packet = riak_pb_codec:encode({rpberrorresp, Message, 0}),
    send_message(Packet, State).

%% @doc Formats the terms with the given string and then sends an
%% error message to the client.
-spec send_error(io:format(), list(), #state{}) -> #state{}.
send_error(Format, Terms, State) ->
    send_error(io_lib:format(Format, Terms), State).

%% @doc Sends multiple messages at once.
-spec send_all(module(), [term()], #state{}) -> #state{}.
send_all(_Service, [], State) ->
    State;
send_all(Service, [Reply|Rest], State) ->
    send_all(Service, Rest, send_encoded_message_or_error(Service, Reply, State)).

%% @doc Flushes all buffered replies to the socket.
-spec flush(iodata(), #state{}) -> #state{}.
flush([], State) ->
    %% The buffer was empty, so do a no-op.
    State;
flush(IoData, #state{socket=Sock}=State) ->
    %% Since we do our own framing, don't send a length header
    inet:setopts(Sock, [{packet, raw}]),
    gen_tcp:send(Sock, IoData),
    %% We want to receive messages with 4-byte headers, so set it back
    inet:setopts(Sock, [{packet, 4}]),
    State.

%% @doc Sends an error and immediately flushes the message buffer.
-spec send_error_and_flush(iolist() | format(), #state{}) -> #state{}.
send_error_and_flush(Error, State) ->
    State1 = send_error(Error, State),
    {ok, Data, NewBuffer} = riak_api_pb_frame:flush(State1#state.buffer),
    flush(Data, State1#state{buffer=NewBuffer}).
