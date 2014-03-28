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
         stats/0,
         active_pb_connects/0, active_pb_connects/1]).

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
    case riak_core_stat:stat_system() of
        legacy   -> register_stats_legacy();
        exometer -> register_stats_exometer()
    end.

register_stats_legacy() ->
    [(catch folsom_metrics:delete_metric({?APP, Name})) || {Name, _Type} <- stats()],
    [register_stat(stat_name(Stat), Type) || {Stat, Type} <- stats()],
    riak_core_stat_cache:register_app(?APP, {?MODULE, produce_stats, []}).

register_stats_exometer() ->
    riak_core_stat:register_stats(?APP, stats()).

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist().
get_stats() ->
    case riak_core_stat:stat_system() of
        legacy   -> get_stats_legacy();
        exometer -> get_stats_exometer()
    end.

get_stats_legacy() ->
    case riak_core_stat_cache:get_stats(?APP) of
        {ok, Stats, _TS} ->
            Stats;
        Error -> Error
    end.

get_stats_exometer() ->
    riak_core_stat:get_stats(?APP).

produce_stats() ->
    {?APP, riak_core_stat_q:get_stats([riak_api])}.

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
update1(Arg) ->
    case riak_core_stat:stat_system() of
        legacy   -> update1_legacy(Arg);
        exometer -> update1_exometer(Arg)
    end.

update1_legacy(pbc_connect) ->
    folsom_metrics:notify_existing_metric({?APP, pbc_connects}, 1, spiral).

update1_exometer(pbc_connect) ->
    exometer:update([?APP, pbc_connects], 1).

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------
stats() ->
    [
     {pbc_connects, spiral},
     {[pbc_connects, active],
      {function, ?MODULE, active_pb_connects}}
    ].

stat_name(Name) when is_list(Name) ->
    list_to_tuple([?APP] ++ Name);
stat_name(Name) when is_atom(Name) ->
    {?APP, Name}.

register_stat(Name, spiral) ->
    folsom_metrics:new_spiral(Name);
register_stat(Name, {function, _Module, _Function}=Fun) ->
    folsom_metrics:new_gauge(Name),
    folsom_metrics:notify({Name, Fun}).

%% called by legacy stats
active_pb_connects() ->
    %% riak_api_pb_sup will not be running when there are no listeners
    %% defined.
    case erlang:whereis(riak_api_pb_sup) of
        undefined -> 0;
        _ -> proplists:get_value(active, supervisor:count_children(riak_api_pb_sup))
    end.

%% called by exometer (Arg, DataPoints, ignored)
active_pb_connects(_) ->
    %% riak_api_pb_sup will not be running when there are no listeners
    %% defined.
    case erlang:whereis(riak_api_pb_sup) of
        undefined -> [{value, 0}];
        _ ->
	    [{value, proplists:get_value(active, supervisor:count_children(riak_api_pb_sup), 0)}]
    end.
