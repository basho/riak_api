%% -------------------------------------------------------------------
%%
%% riak_api_sup: supervise the Riak API services
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

%% @doc supervise the Riak API services

-module(riak_api_sup).

-behaviour(supervisor).

-export([start_link/0,
         init/1]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).
-define(LNAME(IP, Port), lists:flatten(io_lib:format("~p:~p", [IP, Port]))).
-define(LISTENER(IP, Port), {?LNAME(IP, Port),
                             {riak_api_pb_listener, start_link, [IP, Port]}, 
                             permanent, 5000, worker, [riak_api_pb_listener]}).
%% @doc Starts the supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc The init/1 supervisor callback, initializes the supervisor.
-spec init(list()) -> {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore when
      RestartStrategy :: supervisor:strategy(),
      MaxR :: pos_integer(),
      MaxT :: pos_integer(),
      ChildSpec :: supervisor:child_spec().
init([]) ->
    Listeners = riak_api_pb_listener:get_listeners(),
    Helper = ?CHILD(riak_api_pb_registration_helper, worker),
    Registrar = ?CHILD(riak_api_pb_registrar, worker),
    NetworkProcesses = if Listeners /= [] ->
                               [?CHILD(riak_api_pb_sup, supervisor)] ++
                                   listener_specs(Listeners);
                          true ->
                               lager:info("No PB listeners were configured,"
                                          " PB connections will be disabled."),
                               []
                       end,
    {ok, {{one_for_one, 10, 10}, [Helper, Registrar|NetworkProcesses]}}.

listener_specs(Pairs) ->
    [ ?LISTENER(IP, Port) || {IP, Port} <- Pairs ].
