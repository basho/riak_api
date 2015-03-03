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

-module(riak_api_lib).
-export([get_active_listener_entrypoints/1, which_eth0/0]).

-type proto() :: http|pbc.

-spec get_active_listener_entrypoints(proto()) ->
    {ExtIP::string() | none, [Port::non_neg_integer()]}.
%% @doc Collect the ports of all pbc and http listeners set up
%%      at this node and discover the ips bound on configured network
%%      interfaces.  Used to provide API entry points coverage for
%%      external clients.
get_active_listener_entrypoints(Proto) ->
    %% 0. collect ports
    Ports =
        case Proto of
            http ->
                [P || {_proto, {_useless_listening_address, P}}
                          <- riak_api_web:get_listeners()];
            pbc ->
                [P || {_, P} <- riak_api_pb_listener:get_listeners()]
        end,
    %% 1. get the ip bound on the first non-lo interface
    {which_eth0(), Ports}.


-spec which_eth0() -> string() | none.
%% @doc Returns the ip address bound on the first non-lo interface
%%      in the underlying OS.
which_eth0() ->
    UpIfaces =
        case inet:getifaddrs() of
            {ok, Ifaces} ->
                lists:filtermap(
                  fun({"lo", _}) ->
                          false;
                     ({_eth0, Details}) ->
                          case proplists:get_value(addr, Details) of
                              undefined ->
                                  false;
                              Addr ->
                                  {true, inet:ntoa(Addr)}
                          end
                  end,
                  Ifaces);
            {error, PosixCode} ->
                _ = lager:log(error, self(), "Failed to enumerate network ifaces: ~p", [PosixCode]),
                []
        end,
    case length(UpIfaces) of
        1 ->
            %% single externally accessible ip, a common and sound setup
            hd(UpIfaces);
        0 ->
            %% dark node!
            _ = lager:log(warning, self(), "No interfaces with bound ip found", []),
            none;
        Many ->
            %% Admins got carried away.
            %% To be perfectly accurate, it would probably make sense to match
            %% these IPs to those returned by get_listeners and exclude the IPs
            %% not registered by the listeners.  I can imagine a setup where
            %% erlang nodes are set up to communicate over infiniband while
            %% some controlling traffic is routed over eth0.
            %%
            %% Be done with a log warning, for now.
            _ = lager:log(
              warning, self(), "Multiple interfaces (~b) with bound ip found; returning the first",
              [Many]),
            hd(UpIfaces)
    end.
