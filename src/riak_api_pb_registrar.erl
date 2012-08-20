%% -------------------------------------------------------------------
%%
%% riak_api_pb_registrar: Riak Client APIs Protocol Buffers Service Registration
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

%% @doc Encapsulates the Protocol Buffers service registration and
%% deregistration as a gen_server process. This is used to serialize
%% write access to the registration table so that it is less prone to
%% race-conditions.

-module(riak_api_pb_registrar).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).
-endif.

-define(SERVER, ?MODULE).

%% External exports
-export([
         start_link/0,
         register/1,
         deregister/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-import(riak_api_pb_service, [services/0, dispatch_table/0]).

%%--------------------------------------------------------------------
%%% Public API
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec register([riak_api_pb_service:registration()]) -> ok | {error, Reason::term()}.
register(Registrations) ->
    gen_server:call(?SERVER, {register, Registrations}, infinity).

-spec deregister([riak_api_pb_service:registration()]) -> ok | {error, Reason::term()}.
deregister(Registrations) ->
    gen_server:call(?SERVER, {deregister, Registrations}, infinity).

%%--------------------------------------------------------------------
%%% gen_server callbacks
%%--------------------------------------------------------------------
init([]) ->
    {ok, undefined}.

handle_call({register, Registrations}, _From, State) ->
    Reply = do_register(Registrations),
    {reply, Reply, State};
handle_call({deregister, Registrations}, _From, State) ->
    Reply = do_deregister(Registrations),
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

do_register([]) ->
    ok;
do_register([{Module, MinCode, MaxCode}|Rest]) ->
    case do_register(Module, MinCode, MaxCode) of
        ok ->
            do_register(Rest);
        Error ->
            Error
    end.

do_register(_Module, MinCode, MaxCode) when MinCode > MaxCode orelse
MinCode < 1 orelse
MaxCode < 1 ->
    {error, invalid_message_code_range};
do_register(Module, MinCode, MaxCode) ->
    Registrations = dispatch_table(),
    IsRegistered = fun(I) -> dict:is_key(I, Registrations) end,
    CodeRange = lists:seq(MinCode, MaxCode),
    case lists:filter(IsRegistered, CodeRange) of
        [] ->
            NewRegs = lists:foldl(fun(I, D) ->
                                          dict:store(I, Module, D)
                                  end, Registrations, CodeRange),
            application:set_env(riak_api, services, NewRegs),
            riak_api_pb_sup:service_registered(Module),
            ok;
        AlreadyClaimed ->
            {error, {already_registered, AlreadyClaimed}}
    end.


do_deregister([]) ->
    ok;
do_deregister([{Module, MinCode, MaxCode}|Rest]) ->
    case do_deregister(Module, MinCode, MaxCode) of
        ok ->
            do_deregister(Rest);
        Other ->
            Other
    end.

do_deregister(_Module, MinCode, MaxCode) when MinCode > MaxCode orelse
MinCode < 1 orelse
MaxCode < 1 ->
    {error, invalid_message_code_range};
do_deregister(Module, MinCode, MaxCode) ->
    Registrations = dispatch_table(),
    CodeRange = lists:seq(MinCode, MaxCode),
    %% Figure out whether all of the codes can be deregistered.
    Mapper = fun(I) ->
                     case dict:find(I, Registrations) of
                         error ->
                             {error, {unregistered, I}};
                         {ok, Module} ->
                             I;
                         {ok, _OtherModule} ->
                             {error, {not_owned, I}}
                     end
             end,
    ToRemove = [ Mapper(I) || I <- CodeRange ],
    case ToRemove of
        CodeRange ->
            %% All codes are valid, so remove them, set the env and
            %% notify active server processes.
            NewRegs = lists:foldl(fun dict:erase/2, Registrations, CodeRange),
            application:set_env(riak_api, services, NewRegs),
            riak_api_pb_sup:service_registered(Module),
            ok;
        _ ->
            %% There was at least one error, return it.
            lists:keyfind(error, 1, ToRemove)
    end.



-ifdef(TEST).

test_start() ->
    gen_server:start({local, ?SERVER}, ?MODULE, [], []).

setup() ->
    OldServices = app_helper:get_env(riak_api, services, dict:new()),
    application:set_env(riak_api, services, dict:new()),
    {ok, Pid} = test_start(),
    {Pid, OldServices}.

cleanup({Pid, Services}) ->
    exit(Pid, kill),
    application:set_env(riak_api, services, Services).

deregister_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Deregister a previously registered service
      ?_assertEqual(ok, begin
                            ok = riak_api_pb_service:register(foo, 1, 2),
                            riak_api_pb_service:deregister(foo, 1, 2)
                        end),
      %% Invalid deregistration: range is invalid
      ?_assertEqual({error, invalid_message_code_range}, riak_api_pb_service:deregister(foo, 2, 1)),
      %% Invalid deregistration: unregistered range
      ?_assertEqual({error, {unregistered, 1}}, riak_api_pb_service:deregister(foo, 1, 1)),
      %% Invalid deregistration: registered to other service
      ?_assertEqual({error, {not_owned, 1}}, begin
                                                 ok = riak_api_pb_service:register(foo, 1, 2),
                                                 riak_api_pb_service:deregister(bar, 1)
                                             end),
      %% Deregister multiple
      ?_assertEqual(ok, begin
                            ok = register([{foo, 1, 2}, {bar, 3, 4}]),
                            deregister([{bar, 3, 4}, {foo, 1, 2}])
                        end)
     ]}.

register_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Valid registration range
      ?_assertEqual(foo, begin
                             ok = riak_api_pb_service:register(foo,1,2),
                             dict:fetch(1, dispatch_table())
                         end),
      %% Registration ranges that are invalid
      ?_assertEqual({error, invalid_message_code_range},
                    riak_api_pb_service:register(foo, 2, 1)),
      ?_assertEqual({error, {already_registered, [1, 2]}},
                    begin
                        ok = riak_api_pb_service:register(foo, 1, 2),
                        riak_api_pb_service:register(bar, 1, 3)
                    end),
      %% Register multiple
      ?_assertEqual(ok, register([{foo, 1, 2}, {bar, 3, 4}]))
     ]}.

services_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      ?_assertEqual([], services()),
      ?_assertEqual([bar, foo], begin
                                    riak_api_pb_service:register(foo, 1, 2),
                                    riak_api_pb_service:register(bar, 3, 4),
                                    services()
                                end)
     ]}.

-endif.
