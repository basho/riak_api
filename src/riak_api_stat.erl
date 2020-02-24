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
-export([
      start_link /0, register_stats/0,
      get_stats/0,
      produce_stats/0,
      get_stat/1,
      update/1,
      stats/0,
      active_pb_connects/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
				 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(APP, riak_api).
-define(PREFIX, riak).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
		gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    lists:foreach(fun(Stat) -> stats:register([?PREFIX,?APP|Stat]) end,stats()).

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist().
get_stats() ->
    get_stat(?APP).

produce_stats() ->
		{?APP, get_value(?APP)}.

get_stat(Arg) ->
    stats:get_stats(Arg).

get_value(Arg) ->
    stats:get_value(Arg).

%% -------------------------------------------------------------------


update(Arg) ->
    Statname = lists:flatten([?PREFIX,?APP | Arg]),
    update1(Statname).

%% gen_server

init([]) ->
		register_stats(),
		{ok, ok}.

handle_call(_Req, _From, State) ->
		{reply, ok, State}.

handle_cast(_Req, State) ->
		{noreply, State}.

handle_info(_Info, State) ->
		{noreply, State}.

terminate(_Reason, _State) ->
		ok.

code_change(_OldVsn, State, _Extra) ->
		{ok, State}.

%% @doc Update the given `Stat'. Only one stat is updated and it is a
%%      spiral @end
-spec update1(term()) -> ok.
update1(StatName) ->
    stats:update(StatName, 1, spiral).

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------
stats() ->
		[
		 {[pbc, connects], spiral, [], [{one, pbc_connects},
																 {count, pbc_connects_total}]},
		 {[pbc, connects, active], {function, ?MODULE, active_pb_connects}, [], [{value, pbc_active}]}
		].

active_pb_connects(_) ->
		%% riak_api_pb_sup will not be running when there are no listeners
		%% defined.
		case erlang:whereis(riak_api_pb_sup) of
				undefined -> [{value, 0}];
				_ ->
			[{value, proplists:get_value(active, supervisor:count_children(riak_api_pb_sup), 0)}]
		end.
