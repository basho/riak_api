%% -------------------------------------------------------------------
%%
%% riak_api_app: Riak Client APIs
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

%% @doc Bootstrapping the Riak Client APIs application.
-module(riak_api_app).

-behaviour(application).
-export([start/2,
         stop/1]).

-define(SERVICES, [{riak_api_basic_pb_service, 1, 2},
                   {riak_api_basic_pb_service, 7, 8}]).

%% @doc The application:start callback.
-spec start(Type, StartArgs)
           -> {ok, Pid} | {ok, Pid, State} | {error, Reason} when
      Type :: normal
             | {takeover, Node :: node()}
             | {failover, Node :: node()},
      Pid :: pid(),
      State :: term(),
      StartArgs :: term(),
      Reason :: term().
start(_Type, _StartArgs) ->
    riak_core_util:start_app_deps(riak_api),

    %% TODO: cluster_info registration. What do we expose?
    %% catch cluster_info:register_app(riak_api_cinfo),

    ok = riak_api_pb_service:register(?SERVICES),

    case riak_api_sup:start_link() of
        {ok, Pid} ->
            %% TODO: Is it necessary to register the service? We might
            %% want to use the registration to cause service_up events
            %% and then propagate config information for client
            %% auto-config.
            %% riak_core:register(riak_api, []),
            %% register stats
            riak_core:register(riak_api, [{stat_mod, riak_api_stat}]),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc The application:stop callback.
-spec stop(State::term()) -> ok.
stop(_State) ->
    ok = riak_api_pb_service:deregister(?SERVICES),
    ok.
