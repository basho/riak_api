%% -------------------------------------------------------------------
%%
%% riak_api_pb_service: Riak Client APIs Protocol Buffers Services
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

%% @doc Encapsulates the behaviour and registration of
%% application-specific interfaces exposed over the Protocol Buffers
%% API. Service modules should implement the behaviour, and the host
%% applications should register them on startup like so:
%% <pre>
%%   %% Register the 'ping' messages
%%   ok = riak_api_pb_service:register(riak_core_pb_service, 1, 2)
%% </pre>
%% @end

-module(riak_api_pb_service).
-compile([{no_auto_import, [register/2]}]).

%% Behaviour API
-export([behaviour_info/1]).

%% Service-provider API
-export([register/1,
         register/2,
         register/3,
         deregister/1,
         deregister/2,
         deregister/3]).

%% Server API
-export([dispatch_table/0,
         services/0]).

-type registration() :: {Service::module(), MinCode::pos_integer(), MaxCode::pos_integer()}.

-export_type([registration/0]).

%% @doc Behaviour information callback. PB API services must implement
%% the given functions.
behaviour_info(callbacks) ->
    [{init,0},
     {decode,2},
     {encode,1},
     {process,2},
     {process_stream,3}];
behaviour_info(_) ->
    undefined.

%% @doc Registers a number of services at once.
%% @see register/3
-spec register([registration()]) -> ok | {error, Reason::term()}.
register([]) ->
    ok;
register(List) ->
    riak_api_pb_registrar:register(List).

%% @doc Registers a service module for a given message code.
%% @equiv register(Module, Code, Code)
%% @see register/3
-spec register(Module::module(), Code::pos_integer()) -> ok | {error, Err::term()}.
register(Module, Code) ->
    register([{Module, Code, Code}]).

%% @doc Registers a service module for a given range of message
%% codes. The service module must implement the behaviour and be able
%% to decode and process messages for the given range of message
%% codes.  Service modules should be registered before the riak_api
%% application starts.
-spec register(Module::module(), MinCode::pos_integer(), MaxCode::pos_integer()) -> ok | {error, Err::term()}.
register(Module, MinCode, MaxCode) ->
    register([{Module, MinCode, MaxCode}]).

%% @doc Removes the registration of a number of services modules at
%% once.
%% @see deregister/3
-spec deregister([registration()]) -> ok | {error, Reason::term()}.
deregister([]) ->
    ok;
deregister(List) ->
    riak_api_pb_registrar:deregister(List).

%% @doc Removes the registration of a previously-registered service
%% module. Inputs will be validated such that the registered module
%% must match the one being removed.
-spec deregister(Module::module(), Code::pos_integer()) -> ok | {error, Err::term()}.
deregister(Module, Code) ->
    deregister([{Module, Code, Code}]).

%% @doc Removes the registration of a previously-registered service
%% module.
%% @see deregister/2
-spec deregister(Module::module(), MinCode::pos_integer(), MaxCode::pos_integer()) -> ok | {error, Err::term()}.
deregister(Module, MinCode, MaxCode) ->
    deregister([{Module, MinCode, MaxCode}]).

%% @doc Returns the current mappings from message codes to service
%% modules. This is called by riak_api_pb_socket on startup so that
%% dispatches don't hit the application env.
-spec dispatch_table() -> dict().
dispatch_table() ->
    app_helper:get_env(riak_api, services, dict:new()).

%% @doc Returns the current registered PB services, based on the
%% dispatch_table().
-spec services() -> [ module() ].
services() ->
    lists:usort([ V || {_K,V} <- dict:to_list(dispatch_table()) ]).
