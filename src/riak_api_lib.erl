%% -------------------------------------------------------------------
%%
%% riak_api_lib: Utility functions for riak_api
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Collect API entry point information and cache it in cluster metadata,
%%      via an rpc call to getifaddrs/0 on some or all riak_kv nodes.

-module(riak_api_lib).
-export([get_routed_interfaces/0,
         get_entrypoints/1,
         get_entrypoints/2]).
-export([get_entrypoints_local/1]).  %% called via rpc:call

-type proto() :: http|pbc.
-type maybe_routed_address() :: inet:ip_address() | no_interfaces | not_routed.
-type ep() :: [{addr, maybe_routed_address()} | {port, inet:port_number()} |
               {proto, proto()} | {last_checked, {integer(), integer(), integer()}}].
-type get_entrypoints_options() :: [{force_update, boolean()} |
                                    {restrict_nodes, [node()]|all}].
-export_type([proto/0, maybe_routed_address/0, ep/0,
             get_entrypoints_options/0]).

-define(EP_META_PREFIX, {api, entrypoint}).
-define(RPC_TIMEOUT, 10000).

-define(plget, proplists:get_value).


-spec get_entrypoints(proto()) -> [{node(), [ep()]}] | undefined.
%% @doc Wrapper for get_entrypoints(Proto, [{force_update, false},
%%      {restrict_nodes, all}]).
get_entrypoints(Proto) ->
    get_entrypoints(Proto, [{force_update, false}, {restrict_nodes, all}]).

-spec get_entrypoints(proto(), get_entrypoints_options()) -> [{node(), [ep()]}] | undefined.
%% @doc Return (and also collect unless already cached and
%%      'force_update' property is false) a per-node proplist with
%%      proplists with keys: addr, port, last_checked, of pbc or http
%%      listeners on all or some nodes.
get_entrypoints(Proto, Options) ->
    Nodes =
        case ?plget(restrict_nodes, Options, all) of
            all ->
                riak_core_node_watcher:nodes(riak_kv);
            Subset ->
                Subset
        end,
    case ?plget(force_update, Options, false) of
        true ->
            {ResL, FailedNodes} =
                rpc:multicall(
                  Nodes, ?MODULE, get_entrypoints_local,
                  [Proto], ?RPC_TIMEOUT),
            case FailedNodes of
                [] ->
                    fine;
                FailedNodes ->
                    ok = lager:log(
                           warning, self(), "Failed to get ~p riak api listeners at node(s) ~999p",
                           [Proto, FailedNodes])
            end,
            GoodNodes = Nodes -- FailedNodes,
            EntryPoints = lists:zip(GoodNodes, flatten_one_level(ResL)),
            metadata_put_entrypoints(Proto, EntryPoints),
            EntryPoints;
        _ ->
            case metadata_get_entrypoints(Proto, Nodes) of
                PartialOrUndefined
                  when PartialOrUndefined == undefined
                       orelse length(PartialOrUndefined) /= length(Nodes) ->
                    get_entrypoints(
                      Proto, lists:keystore(force_update, 1, Options,
                                            {force_update, true}));
                FullSet ->
                    FullSet
            end
    end.


-spec get_routed_interfaces() -> [{Iface::string(), inet:ip_address()}].
%% @doc Returns the {iface, address} pairs of all non-loopback, bound
%%      interfaces in the underlying OS.
get_routed_interfaces() ->
    case inet:getifaddrs() of
        {ok, Ifaces} ->
            lists:filtermap(
              fun({Iface, Details}) ->
                      case is_routed_addr(Details) of
                          undefined ->
                              false;
                          Addr ->
                              {true, {Iface, Addr}}
                      end
              end,
              Ifaces);
        {error, PosixCode} ->
            _ = lager:log(error, self(), "Failed to enumerate network ifaces: ~p", [PosixCode]),
            []
    end.


%% ===================================================================
%% Local functions
%% ===================================================================

-spec is_routed_addr([{Ifname::string(), Ifopt::[{atom(), any()}]}]) ->
    inet:ip_address() | undefined.
%% @private
is_routed_addr(Details) ->
    Flags = ?plget(flags, Details),
    case {(is_list(Flags) andalso
           %% andalso lists:member(running, Flags)
           %% iface is reported as 'running' when it's not according
           %% to ifconfig -- why?
           not lists:member(loopback, Flags)),
          proplists:get_all_values(addr, Details)} of
        {true, [_|_] = Ipv4AndPossibly6} ->
            %% prefer the ipv4 addr (4-elem tuple < 6-elem tuple),
            %% only select ipv6 if ipv4 is missing
            hd(lists:sort(Ipv4AndPossibly6));
        _ ->
            undefined
    end.


-spec get_entrypoints_local(proto()) -> [ep()].
%% @private
%% Works locally to determine bound addresses of all active listeners.
get_entrypoints_local(Proto) ->
    ListenerDetails =
        case Proto of
            http ->
                [HP || {_proto, HP = {_listening_address_as_string, _port}}
                          <- riak_api_web:get_listeners()];
            pbc ->
                %% good as is
                riak_api_pb_listener:get_listeners()
        end,
    Now = os:timestamp(),
    [[{addr, BoundAddr}, {port, Port}, {last_checked, Now}] ||
        {BoundAddr, Port} <- figure_routed_addresses(ListenerDetails)].


is_addr_wildcard(A) when A == {0,0,0,0}; A == {0,0,0,0, 0,0,0,0} ->
    true;
is_addr_wildcard(S) when is_list(S) ->
    {ok, A} = inet:parse_address(S),
    is_addr_wildcard(A);
is_addr_wildcard(_) ->
    false.


-spec figure_routed_addresses([{inet:ip_address(), inet:port_number()}]) ->
    [{maybe_routed_address(), inet:port_number()}].
%% @private
%% Find suitable routed addresses for listening ones:
%% - if a listener has it spelled out, take that, but check that it
%%   matches some of the bound addresses;
%% - if a listener has 0.0.0.0, take the first non-loopback address.
figure_routed_addresses(ListenerDetails) ->
    {_ifaces, BoundAddresses} =
        lists:unzip(get_routed_interfaces()),
    lists:map(
      fun({Addr, Port}) ->
              case {is_addr_wildcard(Addr), lists:member(Addr, BoundAddresses)} of
                  {true, _} when length(BoundAddresses) == 0 ->
                      ok = lager:log(
                             warning, self(),
                             "API listener at *:~b not accessible because node ~s has no routed addresses",
                             [Port, node()]),
                      {no_interfaces, Port};
                  {true, _} ->
                      {hd(BoundAddresses), Port};
                  {false, true} ->
                      {Addr, Port};  %% as configured
                  {false, false} ->
                      ok = lager:log(
                             warning, self(),
                             "API listener at ~s:~b not accessible via any routed addresses (~s)",
                             [Addr, Port,
                              string:join(lists:map(fun inet:ntoa/1, BoundAddresses), ", ")]),
                      {not_routed, Port}
              end
      end,
      ListenerDetails).


-spec metadata_put_entrypoints(proto(), [{node(), [ep()]}]) -> ok.
%% @private
%% Merge with update NewList with CachedList
metadata_put_entrypoints(Proto, NewList) ->
    case riak_core_metadata:get(?EP_META_PREFIX, Proto) of
        undefined ->
            ok = riak_core_metadata:put(?EP_META_PREFIX, Proto, NewList),
            NewList;
        CachedList ->
            UpdatedList =
                lists:foldl(
                  fun(Updating = {Node, _}, Acc) ->
                          lists:keystore(Node, 1, Acc, Updating)
                  end,
                  CachedList,
                  NewList),
            ok = riak_core_metadata:put(?EP_META_PREFIX, Proto, UpdatedList)
    end.

-spec metadata_get_entrypoints(proto(), [node()]) -> [{node(), [ep()]}] | undefined.
%% @private
metadata_get_entrypoints(Proto, Nodes) ->
    case riak_core_metadata:get(?EP_META_PREFIX, Proto) of
        undefined ->
            undefined;
        FullList ->
            lists:filter(
              fun({K, _}) -> lists:member(K, Nodes) end,
              FullList)
            %% can result in a partial set
    end.


-spec flatten_one_level([any()]) -> [any()].
%% @private
flatten_one_level(L) ->
    lists:map(fun([M]) -> M; (M) -> M end, L).
