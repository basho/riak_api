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

-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("public_key/include/public_key.hrl").

-behaviour(gen_fsm).

%% API
-export([start_link/0, set_socket/2, service_registered/2]).

%% States
-export([wait_for_socket/2, wait_for_socket/3, wait_for_tls/2, wait_for_tls/3,
         wait_for_auth/2, wait_for_auth/3, connected/2, connected/3]).

-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-record(state, {
          transport = {gen_tcp, inet} :: {gen_tcp, inet} | {ssl, ssl},
          socket :: port() | ssl:sslsocket(),   % socket
          req,                % current request
          states :: orddict:orddict(),    % per-service connection state
          peername :: undefined | {inet:ip_address(), pos_integer()},
          common_name :: undefined | string(),
          security,
          retries = 3,
          inbuffer = <<>>, % when an incomplete message comes in, we have to unpack it ourselves
          outbuffer = riak_api_pb_frame:new() :: riak_api_pb_frame:buffer() % frame buffer which we can use to optimize TCP sends
         }).

-type format() :: {format, term()} | {format, io:format(), [term()]}.
-export_type([format/0]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Starts a PB server, ready to service a single socket.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_fsm:start_link(?MODULE, [], []).

%% @doc Sets the socket to service for this server.
-spec set_socket(pid(), port()) -> ok.
set_socket(Pid, Socket) ->
    gen_fsm:sync_send_event(Pid, {set_socket, Socket}, infinity).

%% @doc Notifies the server process of a newly registered PB service.
-spec service_registered(pid(), module()) -> ok.
service_registered(Pid, Mod) ->
    gen_fsm:send_all_state_event(Pid, {registered, Mod}).

%% @doc The gen_server init/1 callback, initializes the
%% riak_api_pb_server.
-spec init(list()) -> {ok, wait_for_socket, #state{}}.
init([]) ->
    riak_api_stat:update(pbc_connect),
    ServiceStates = lists:foldl(fun(Service, States) ->
                                        orddict:store(Service, Service:init(), States)
                                end,
                                orddict:new(),
                                riak_api_pb_registrar:services()),
    {ok, wait_for_socket, #state{states=ServiceStates}}.

wait_for_socket(_Event, State) ->
    {next_state, wait_for_socket, State}.

wait_for_socket({set_socket, Socket}, _From, State=#state{transport={_Transport,Control}}) ->
    case Control:peername(Socket) of
        {ok, PeerInfo} ->
            Control:setopts(Socket, [{active, once}]),
            %% check if security is enabled, if it is wait for TLS, otherwise go
            %% straight into connected state
            case riak_core_security:is_enabled() of
                true ->
                    {reply, ok, wait_for_tls, State#state{socket=Socket,
                                                          peername=PeerInfo}};
                false ->
                    {reply, ok, connected, State#state{socket=Socket,
                                                       peername=PeerInfo}}
            end;
        {error, Reason} ->
            lager:debug("Could not get PB socket peername: ~p", [Reason]),
            %% It's not really "ok", but there's no reason for the
            %% listener to crash just because this socket had an
            %% error. See riak_api#54.
            {stop, normal, ok, State}
    end;
wait_for_socket(_Event, _From, State) ->
    {reply, unknown_message, wait_for_socket, State}.

wait_for_tls({msg, MsgCode, _MsgData}, State=#state{socket=Socket,
                                                    transport={Transport, _Control}}) ->
    case riak_pb_codec:msg_code(rpbstarttls) of
        MsgCode ->
            %% got STARTTLS msg, send ACK back to client
            Transport:send(Socket, <<1:32/unsigned-big, MsgCode:8>>),
            %% now do the SSL handshake
            CACerts = riak_core_ssl_util:load_certs(app_helper:get_env(riak_api,
                                                                       cacertfile)),
            {Ciphers, _} =
                riak_core_ssl_util:parse_ciphers(riak_core_security:get_ciphers()),
            case ssl:ssl_accept(Socket, [{certfile,
                                          app_helper:get_env(riak_api,
                                                                       certfile)},
                                         {keyfile, app_helper:get_env(riak_api,
                                                                      keyfile)},
                                         {cacerts, CACerts},
                                         {ciphers, Ciphers},
                                         {versions,
                                          app_helper:get_env(riak_api,
                                                             tls_protocols,
                                                             ['tlsv1.2'])},
                                         %% force peer validation, even though
                                         %% we don't care if the peer doesn't
                                         %% send a certificate
                                         {verify, verify_peer},
                                         {reuse_sessions, false} %% required!
                                        ] ++
                                        %% conditionally include the honor cipher order, don't pass it if it
                                        %% disabled because it will crash any
                                        %% OTP installs that lack the patch to
                                        %% implement honor_cipher_order
                                        [{honor_cipher_order, true} ||
                                         app_helper:get_env(riak_api,
                                                            honor_cipher_order,
                                                            false) ] ++
                                        %% if we're validating CRLs, define a
                                        %% verify_fun for them.
                                        [{verify_fun, {fun validate_function/3,
                                                       {CACerts, []}}} ||
                                         app_helper:get_env(riak_api,
                                                            check_crl, false)]

                               ) of
                {ok, NewSocket} ->
                    CommonName = case ssl:peercert(NewSocket) of
                        {ok, Cert} ->
                            OTPCert = public_key:pkix_decode_cert(Cert, otp),
                            riak_core_ssl_util:get_common_name(OTPCert);
                        {error, _Reason} ->
                            undefined
                    end,
                    lager:debug("STARTTLS succeeded, peer's common name was ~p",
                               [CommonName]),
                    {next_state, wait_for_auth,
                     State#state{socket=NewSocket, common_name=CommonName, transport={ssl,ssl}}};
                {error, Reason} ->
                    lager:warning("STARTTLS with client ~s failed: ~p",
                                  [format_peername(State#state.peername), Reason]),
                    {stop, {error, {startls_failed, Reason}}, State}
            end;
        _ ->
            lager:debug("Client sent unexpected message code ~p", [MsgCode]),
            State1 = send_error_and_flush("Security is enabled, please STARTTLS first",
                                 State),
            {next_state, wait_for_tls, State1}
    end;
wait_for_tls(_Event, State) ->
    {next_state, wait_for_tls, State}.

wait_for_tls(_Event, _From, State) ->
    {reply, unknown_message, wait_for_tls, State}.

wait_for_auth({msg, MsgCode, MsgData}, State=#state{socket=Socket,
                                                    transport={Transport,_Control}}) ->
    case riak_pb_codec:msg_code(rpbauthreq) of
        MsgCode ->
            %% got AUTH message, try to validate credentials
            AuthReq = riak_pb_codec:decode(MsgCode, MsgData),
            User = AuthReq#rpbauthreq.user,
            Password = AuthReq#rpbauthreq.password,
            {PeerIP, _PeerPort} = State#state.peername,
            case riak_core_security:authenticate(User, Password, [{ip,
                                                                   PeerIP},
                                                                  {common_name,
                                                                   State#state.common_name}]) of
                {ok, SecurityContext} ->
                    lager:debug("authentication for ~p from ~p succeeded",
                               [User, PeerIP]),
                    AuthResp = riak_pb_codec:msg_code(rpbauthresp),
                    Transport:send(Socket, <<1:32/unsigned-big, AuthResp:8>>),
                    {next_state, connected,
                     State#state{security=SecurityContext}};
                {error, Reason} ->
                    %% Allow the client to reauthenticate, I guess?

                    %% Add a delay to make brute-force attempts more annoying
                    timer:sleep(5000),
                    State1 = send_error_and_flush("Authentication failed",
                                                  State),
                    lager:debug("authentication for ~p from ~p failed: ~p",
                               [User, PeerIP, Reason]),
                    case State#state.retries of
                        N when N =< 1 ->
                            %% no more chances
                            {stop, normal, State};
                        Retries ->
                            {next_state, wait_for_auth,
                             State1#state{retries=Retries-1}}
                    end
            end;
        _ ->
            State1 = send_error_and_flush("Security is enabled, please "
                                          "authenticate first", State),
            {next_state, wait_for_auth, State1}
    end;
wait_for_auth(_Event, State) ->
    {next_state, wait_for_auth, State}.

wait_for_auth(_Event, _From, State) ->
    {reply, unknown_message, wait_for_auth, State}.

connected(timeout, State=#state{outbuffer=Buffer}) ->
    %% Flush any protocol messages that have been buffering
    {ok, Data, NewBuffer} = riak_api_pb_frame:flush(Buffer),
    {next_state, connected, flush(Data, State#state{outbuffer=NewBuffer})};
connected({msg, MsgCode, MsgData}, State=#state{states=ServiceStates}) ->
    try
        %% First find the appropriate service module to dispatch
        NewState = case riak_api_pb_registrar:lookup(MsgCode) of
            {ok, Service} ->
                ServiceState = orddict:fetch(Service, ServiceStates),
                %% Decode the message according to the service
                case Service:decode(MsgCode, MsgData) of
                    {ok, Message} ->
                        %% Process the message
                        process_message(Service, Message, ServiceState, State);
                    {ok, Message, Permissions} ->
                        case State#state.security of
                            undefined ->
                                process_message(Service, Message, ServiceState, State);
                            SecCtx ->
                                case riak_core_security:check_permissions(
                                        Permissions, SecCtx) of
                                    {true, NewCtx} ->
                                        process_message(Service, Message,
                                                        ServiceState,
                                                        State#state{security=NewCtx});
                                    {false, Error, NewCtx} ->
                                        send_error(Error,
                                                   [],
                                                   State#state{security=NewCtx})
                                end
                        end;
                    {error, Reason} ->
                        send_error("Message decoding error: ~p", [Reason], State)
                end;
            error ->
                send_error("Unknown message code.", State)
        end,
        {next_state, connected, NewState}
    catch
        %% Tell the client we errored before closing the connection.
        Type:Failure ->
            Trace = erlang:get_stacktrace(),
            FState = send_error_and_flush({format, "Error processing incoming message: ~p:~p:~p",
                                           [Type, Failure, Trace]}, State),
            {stop, {Type, Failure, Trace}, FState}
    end;
connected(_Event, State) ->
    {next_state, connected, State}.

connected(_Event, _From, State) ->
    {reply, unknown_message, connected, State}.

%% @doc The handle_event/3 gen_fsm callback.
handle_event({registered, Service}, StateName, #state{states=ServiceStates}=State) ->
    %% When a new service is registered after a client connection is
    %% already established, update the internal state to support the
    %% new capabilities.
    case orddict:is_key(Service, ServiceStates) of
        true ->
            %% This is an existing service registering
            %% disjoint message codes
            {next_state, StateName, State, 0};
        false ->
            %% This is a new service registering
            {next_state, StateName,
             State#state{states=orddict:store(Service, Service:init(),
                                              ServiceStates)}, 0}
    end;
handle_event(_Msg, StateName, State) ->
    {next_state, StateName, State, 0}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, unknown_message, StateName, State}.

%% @doc The handle_info/3 gen_fsm callback.
handle_info({tcp_closed, Socket}, _SN, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({ssl_closed, Socket}, _SN, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, _Reason}, _SN, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({ssl_error, Socket, _Reason}, _SN, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({Proto, Socket, Bin}, StateName, State=#state{req=undefined,
                                              socket=Socket,
                                              inbuffer=InBuffer}) when
        Proto == tcp; Proto == ssl ->
    %% Because we do our own outbound framing, we need to do our own
    %% inbound deframing.
    NewBuffer = <<InBuffer/binary, Bin/binary>>,
    decode_buffer(StateName, State#state{inbuffer=NewBuffer});
handle_info({Proto, Socket, _Data}, _SN, State=#state{socket=Socket}) when
        Proto == tcp; Proto == ssl ->
    %% req =/= undefined: received a new request while another was in
    %% progress -> Error
    lager:debug("Received a new PB socket request"
                " while another was in progress"),
    State1 = send_error_and_flush("Cannot send another request while one is in progress", State),
    {stop, normal, State1};
handle_info(StreamMessage, StateName, #state{req={Service,ReqId,StreamState}}=State) ->
    %% Handle streaming messages from other processes. This should
    %% help avoid creating extra middlemen. Naturally, this is only
    %% valid when a streaming request has started, other messages will
    %% be ignored.
    try
        NewState = process_stream(Service, ReqId, StreamMessage, StreamState, State),
        {next_state, StateName, NewState, 0}
    catch
        %% Tell the client we errored before closing the connection.
        Type:Reason ->
            Trace = erlang:get_stacktrace(),
            FState = send_error_and_flush({format, "Error processing stream message: ~p:~p:~p",
                                          [Type, Reason, Trace]}, State),
            {stop, {Type, Reason, Trace}, FState}
    end;
handle_info(Message, StateName, State) ->
    %% Throw out messages we don't care about, but log them
    lager:error("Unrecognized message ~p", [Message]),
    {next_state, StateName, State, 0}.


%% @doc The gen_server terminate/2 callback, called when shutting down
%% the server.
-spec terminate(Reason, StateName, State) -> ok when
      Reason :: normal | shutdown | {shutdown,term()} | term(),
      StateName :: atom(),
      State :: #state{}.
terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc The gen_server code_change/3 callback, called when performing
%% a hot code upgrade on the server. Currently unused.
-spec code_change(OldVsn, StateName, State, Extra) -> {ok, StateName, State} when
      OldVsn :: Vsn | {down, Vsn},
      Vsn :: term(),
      StateName :: atom(),
      State :: #state{},
      Extra :: term().
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

decode_buffer(StateName, State=#state{socket=Socket,
                                      transport={_Transport,Control},
                                      inbuffer=Buffer}) ->
    case erlang:decode_packet(4, Buffer, []) of
        {ok, <<MsgCode:8, MsgData/binary>>, Rest} ->
            case ?MODULE:StateName({msg, MsgCode, MsgData}, State) of
                {next_state, NewStateName, NewState} ->
                    decode_buffer(NewStateName, NewState#state{inbuffer=Rest});
                Stop ->
                    Stop
            end;
        {ok, Binary, Rest} ->
            lager:error("Unexpected message format! Message: ~p, Rest: ~p", [Binary, Rest]),
            {stop, badmessage, State};
        {more, _Length} ->
            Control:setopts(Socket, [{active, once}]),
            {next_state, StateName, State, 0};
        {error, Reason} ->
            FState = send_error_and_flush({format, "Invalid message packet, reason: ~p", [Reason]},
                                          State#state{inbuffer= <<>>}),
            {next_state, StateName, FState, 0}
    end.


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
send_message(Bin, #state{outbuffer=Buffer}=State) when is_binary(Bin) orelse is_list(Bin) ->
    case riak_api_pb_frame:add(Bin, Buffer) of
        {ok, Buffer1} ->
            State#state{outbuffer=Buffer1};
        {flush, IoData, Buffer1} ->
            flush(IoData, State#state{outbuffer=Buffer1})
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
%%      error message to the client.
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
flush(IoData, #state{socket=Sock, transport={Transport,_Control}}=State) ->
    Transport:send(Sock, IoData),
    State.

%% @doc Sends an error and immediately flushes the message buffer.
-spec send_error_and_flush(iolist() | format(), #state{}) -> #state{}.
send_error_and_flush(Error, State) ->
    State1 = send_error(Error, State),
    {ok, Data, NewBuffer} = riak_api_pb_frame:flush(State1#state.outbuffer),
    flush(Data, State1#state{outbuffer=NewBuffer}).

format_peername({IP, Port}) ->
    io_lib:format("~s:~B", [inet_parse:ntoa(IP), Port]).

%%%
%%% SSL callback functions and helpers
%%%

%% @doc Validator function for SSL negotiation.
%%
validate_function(Cert, valid_peer, State) ->
    lager:debug("validing peer ~p with ~p intermediate certs",
                [riak_core_ssl_util:get_common_name(Cert),
                 length(element(2, State))]),
    %% peer certificate validated, now check the CRL
    Res = (catch check_crl(Cert, State)),
    lager:debug("CRL validate result for ~p: ~p",
                [riak_core_ssl_util:get_common_name(Cert), Res]),
    {Res, State};
validate_function(Cert, valid, {TrustedCAs, IntermediateCerts}=State) ->
    case public_key:pkix_is_self_signed(Cert) of
        true ->
            %% this is a root cert, no CRL
            {valid, {TrustedCAs, [Cert|IntermediateCerts]}};
        false ->
            %% check is valid CA certificate, add to the list of
            %% intermediates
            Res = (catch check_crl(Cert, State)),
            lager:debug("CRL intermediate CA validate result for ~p: ~p",
                        [riak_core_ssl_util:get_common_name(Cert), Res]),
            {Res, {TrustedCAs, [Cert|IntermediateCerts]}}
    end;
validate_function(_Cert, _Event, State) ->
    {valid, State}.

%% @doc Given a certificate, find CRL distribution points for the given
%%      certificate, fetch, and attempt to validate each CRL through
%%      issuer_function/4.
%%
check_crl(Cert, State) ->
    %% pull the CRL distribution point(s) out of the certificate, if any
    case pubkey_cert:select_extension(?'id-ce-cRLDistributionPoints',
                                      pubkey_cert:extensions_list(Cert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.extensions)) of
        undefined ->
            lager:debug("no CRL distribution points for ~p",
                         [riak_core_ssl_util:get_common_name(Cert)]),
            %% fail; we can't validate if there's no CRL
            no_crl;
        CRLExtension ->
            CRLDistPoints = CRLExtension#'Extension'.extnValue,
            DPointsAndCRLs = lists:foldl(fun(Point, Acc) ->
                            %% try to read the CRL over http or from a
                            %% local file
                            case fetch_point(Point) of
                                not_available ->
                                    Acc;
                                Res ->
                                    [{Point, Res} | Acc]
                            end
                    end, [], CRLDistPoints),
            public_key:pkix_crls_validate(Cert,
                                          DPointsAndCRLs,
                                          [{issuer_fun,
                                            {fun issuer_function/4, State}}])
    end.

%% @doc Given a list of distribution points for CRLs, certificates and
%%      both trusted and intermediary certificates, attempt to build and
%%      authority chain back via build_chain to verify that it is valid.
%%
issuer_function(_DP, CRL, _Issuer, {TrustedCAs, IntermediateCerts}) ->
    %% XXX the 'Issuer' we get passed here is the AuthorityKeyIdentifier,
    %% which we are not currently smart enough to understand
    %% Read the CA certs out of the file
    Certs = [public_key:pkix_decode_cert(DER, otp) || DER <- TrustedCAs],
    %% get the real issuer out of the CRL
    Issuer = public_key:pkix_normalize_name(
            pubkey_cert_records:transform(
                CRL#'CertificateList'.tbsCertList#'TBSCertList'.issuer, decode)),
    %% assume certificates are ordered from root to tip
    case find_issuer(Issuer, IntermediateCerts ++ Certs) of
        undefined ->
            lager:debug("unable to find certificate matching CRL issuer ~p",
                        [Issuer]),
            error;
        IssuerCert ->
            case build_chain({public_key:pkix_encode('OTPCertificate',
                                                     IssuerCert,
                                                     otp),
                              IssuerCert}, IntermediateCerts, Certs, []) of
                undefined ->
                    error;
                {OTPCert, Path} ->
                    {ok, OTPCert, Path}
            end
    end.

%% @doc Attempt to build authority chain back using intermediary
%%      certificates, falling back on trusted certificates if the
%%      intermediary chain of certificates does not fully extend to the
%%      root.
%%
%%      Returns: {RootCA :: #OTPCertificate{}, Chain :: [der_encoded()]}
%%
build_chain({DER, Cert}, IntCerts, TrustedCerts, Acc) ->
    %% check if this cert is self-signed, if it is, we've reached the
    %% root of the chain
    Issuer = public_key:pkix_normalize_name(
            Cert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.issuer),
    Subject = public_key:pkix_normalize_name(
            Cert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject),
    case Issuer == Subject of
        true ->
            case find_issuer(Issuer, TrustedCerts) of
                undefined ->
                    undefined;
                TrustedCert ->
                    %% return the cert from the trusted list, to prevent
                    %% issuer spoofing
                    {TrustedCert,
                     [public_key:pkix_encode(
                                'OTPCertificate', TrustedCert, otp)|Acc]}
            end;
        false ->
            Match = lists:foldl(
                      fun(C, undefined) ->
                              S = public_key:pkix_normalize_name(C#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject),
                              %% compare the subject to the current issuer
                              case Issuer == S of
                                  true ->
                                      %% we've found our man
                                      {public_key:pkix_encode('OTPCertificate', C, otp), C};
                                  false ->
                                      undefined
                              end;
                         (_E, A) ->
                              %% already matched
                              A
                      end, undefined, IntCerts),
            case Match of
                undefined when IntCerts /= TrustedCerts ->
                    %% continue the chain by using the trusted CAs
                    lager:debug("Ran out of intermediate certs, switching to trusted certs~n"),
                    build_chain({DER, Cert}, TrustedCerts, TrustedCerts, Acc);
                undefined ->
                    lager:debug("Can't construct chain of trust beyond ~p",
                                [riak_core_ssl_util:get_common_name(Cert)]),
                    %% can't find the current cert's issuer
                    undefined;
                Match ->
                    build_chain(Match, IntCerts, TrustedCerts, [DER|Acc])
            end
    end.

%% @doc Given a certificate and a list of trusted or intermediary
%%      certificates, attempt to find a match in the list or bail with
%%      undefined.
find_issuer(Issuer, Certs) ->
    lists:foldl(
      fun(OTPCert, undefined) ->
              %% check if this certificate matches the issuer
              Normal = public_key:pkix_normalize_name(
                        OTPCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject),
              case Normal == Issuer of
                  true ->
                      OTPCert;
                  false ->
                      undefined
              end;
         (_E, Acc) ->
              %% already found a match
              Acc
      end, undefined, Certs).

%% @doc Find distribution points for a given CRL and then attempt to
%%      fetch the CRL from the first available.
fetch_point(#'DistributionPoint'{distributionPoint={fullName, Names}}) ->
    Decoded = [{NameType,
                pubkey_cert_records:transform(Name, decode)}
               || {NameType, Name} <- Names],
    fetch(Decoded).

%% @doc Given a list of locations to retrieve a CRL from, attempt to
%%      retrieve either from a file or http resource and bail as soon as
%%      it can be found.
%%
%%      Currently, only hand a armored PEM or DER encoded file, with
%%      defaulting to DER.
%%
fetch([]) ->
    not_available;
fetch([{uniformResourceIdentifier, "file://"++_File}|Rest]) ->
    lager:debug("fetching CRLs from file URIs is not supported"),
    fetch(Rest);
fetch([{uniformResourceIdentifier, "http"++_=URL}|Rest]) ->
    lager:debug("getting CRL from ~p~n", [URL]),
    _ = inets:start(),
    case httpc:request(get, {URL, []}, [], [{body_format, binary}]) of
        {ok, {_Status, _Headers, Body}} ->
            case Body of
                <<"-----BEGIN", _/binary>> ->
                    [{'CertificateList',
                      DER, _}=CertList] = public_key:pem_decode(Body),
                    {DER, public_key:pem_entry_decode(CertList)};
                _ ->
                    %% assume DER encoded
                    CertList = public_key:pem_entry_decode(
                            {'CertificateList', Body, not_encrypted}),
                    {Body, CertList}
            end;
        {error, _Reason} ->
            lager:debug("failed to get CRL ~p~n", [_Reason]),
            fetch(Rest)
    end;
fetch([Loc|Rest]) ->
    %% unsupported CRL location
    lager:debug("unable to fetch CRL from unsupported location ~p",
                [Loc]),
    fetch(Rest).

-ifdef(TEST).

-include("riak_api_pb_registrar.hrl").

receive_closed_socket_test_() ->
    {setup,
     fun() ->
             %% Create the registration table so the server will start up.
             try ets:new(?ETS_NAME, ?ETS_OPTS) of
                 ?ETS_NAME -> true
             catch
                 _:badarg -> false
             end
     end,
     fun(true) -> ets:delete(?ETS_NAME);
        (_) -> ok
     end,
     ?_test(
        begin
            %% Pretend that we're a listener, listen on any port
            {ok, Listen} = gen_tcp:listen(0, []),
            {ok, {Address, Port}} = inet:sockname(Listen),

            %% Connect as a client
            {ok, ClientSocket} = gen_tcp:connect(Address, Port, []),

            %% Accept the socket, start a server, give it over to the server,
            %% then have the client close the socket.
            {ok, ServerSocket} = gen_tcp:accept(Listen),
            {ok, Server} = gen_fsm:start(?MODULE, [], []),
            MRef = monitor(process, Server),
            ok = gen_tcp:controlling_process(ServerSocket, Server),
            ok = gen_tcp:close(ClientSocket),
            timer:sleep(1),

            %% The call to set_socket should reply ok, but shutdown the
            %% server, not crash and propagate back to the listener process.
            ?assertEqual(ok, set_socket(Server, ServerSocket)),
            receive
                {'DOWN', MRef, process, Server, _} -> ok
            after 5000 ->
                    %% We shouldn't miss the DOWN message, but let's
                    %% just check that the process is stopped now.
                    ?assertNot(erlang:is_process_alive(Server))
            end,

            %% Close the listening socket
            gen_tcp:close(Listen)
        end
       )}.

-endif.
