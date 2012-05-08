-module(pb_service_test).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% Implement a dumb PB service
%% ===================================================================
-behaviour(riak_api_pb_service).
-export([init/0,
         decode/2,
         encode/1,
         process/2]).

init() ->
    undefined.

decode(1, <<>>) ->
    {ok, dummyreq};
decode(_,_) ->
    {error, unknown_message}.

encode(ok) ->
    {ok, <<2,$o,$k>>};
encode(_) ->
    error.

process(dummyreq, State) ->
    {reply, ok, State}.

%% ===================================================================
%% Eunit tests
%% ===================================================================
setup() ->
    OldServices = riak_api_pb_service:dispatch_table(),
    OldHost = app_helper:get_env(riak_api, pb_ip, "127.0.0.1"),
    OldPort = app_helper:get_env(riak_api, pb_port, 8087),
    application:set_env(riak_api, services, dict:new()),
    application:set_env(riak_api, pb_ip, "127.0.0.1"),
    application:set_env(riak_api, pb_port, 32767),
    riak_api_pb_service:register(?MODULE, 1, 2),

    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "pb_service_test_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "pb_service_test.log"}),
    application:start(sasl),

    ok = application:start(riak_api),
    {OldServices, OldHost, OldPort}.

cleanup({S, H, P}) ->
    application:stop(riak_api),
    application:set_env(riak_api, services, S),
    application:set_env(riak_api, pb_ip, H),
    application:set_env(riak_api, pb_port, P),
    ok.
    
request(Code, Payload) when is_binary(Payload), is_integer(Code) ->
    Host = app_helper:get_env(riak_api, pb_ip),
    Port = app_helper:get_env(riak_api, pb_port),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active, false}, {packet,4}, 
                                                {header, 1}, {nodelay, true}]),
    ok = gen_tcp:send(Socket, <<Code:8, Payload/binary>>),
    {ok, Response} = gen_tcp:recv(Socket, 0),
    Response.

simple_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      ?_assertEqual([2|<<"ok">>], request(1, <<>>))
     ]}.
