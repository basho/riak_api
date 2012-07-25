%% -------------------------------------------------------------------
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
%%
%% @doc Collector for various api stats.
-module(riak_api_stat).

-behaviour(gen_server).

%% API
-export([start_link /0, register_stats/0,
         get_stats/0,
         produce_stats/0,
         update/1,
         stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(APP, riak_api).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    [(catch folsom_metrics:delete_metric({?APP, Name})) || {Name, _Type} <- stats()],
    [register_stat(Stat, Type) || {Stat, Type} <- stats()],
    riak_core_stat_cache:register_app(?APP, {?MODULE, produce_stats, []}).

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist().
get_stats() ->
    case riak_core_stat_cache:get_stats(?APP) of
        {ok, Stats, _TS} ->
            Stats;
        Error -> Error
    end.

produce_stats() ->
    {?APP, [{Name, get_metric_value({?APP, Name}, Type)} || {Name, Type} <- stats()]}.

update(Arg) ->
    gen_server:cast(?SERVER, {update, Arg}).

%% gen_server

init([]) ->
    register_stats(),
    {ok, ok}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, Arg}, State) ->
    update1(Arg),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Update the given `Stat'.
-spec update1(term()) -> ok.
update1(pbc_connect) ->
    folsom_metrics:notify_existing_metric({?APP, pbc_connects_active}, {inc, 1}, counter),
    folsom_metrics:notify_existing_metric({?APP, pbc_connects}, 1, spiral);
update1(pbc_disconnect) ->
    folsom_metrics:notify_existing_metric({?APP, pbc_connects_active}, {dec, 1}, counter).

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------
get_metric_value(Name, _Type) ->
    folsom_metrics:get_metric_value(Name).

stats() ->
    [
     {pbc_connects, spiral},
     {pbc_connects_active, counter}
    ].

register_stat(Name, spiral) ->
    folsom_metrics:new_spiral({?APP, Name});
register_stat(Name, counter) ->
    folsom_metrics:new_counter({?APP, Name}).
