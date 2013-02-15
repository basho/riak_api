%% -------------------------------------------------------------------
%%
%% riak_core_pb_bucket: Expose Core bucket functionality to Protocol Buffers
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc <p>The Bucket PB service for Riak Core. This is included in
%% the Riak API application because of startup-time constraints. This
%% service covers the following request messages in the original
%% protocol:</p>
%%
%% <pre>
%% 19 - RpbGetBucketReq
%% 21 - RpbSetBucketReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%% 20 - RpbGetBucketResp
%% 22 - RpbSetBucketResp
%% </pre>
%%
%% <p>The semantics are unchanged from their original
%% implementations.</p>
%% @end
-module(riak_core_pb_bucket).

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-include_lib("riak_pb/include/riak_pb.hrl").

init() ->
    undefined.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) when Code == 19; Code == 21 ->
    {ok, riak_pb_codec:decode(Code, Bin)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% Get bucket properties
process(#rpbgetbucketreq{bucket=B}, State) ->
    Props = riak_core_bucket:get_bucket(B),
    PbProps = riak_pb_codec:encode_bucket_props(Props),
    {reply, #rpbgetbucketresp{props = PbProps}, State};

%% Set bucket properties
process(#rpbsetbucketreq{bucket=B, props = PbProps}, State) ->
    Props = riak_pb_codec:decode_bucket_props(PbProps),
    case riak_core_bucket:set_bucket(B, Props) of
        ok ->
            {reply, rpbsetbucketresp, State};
        {error, Details} ->
            {error, {format, "Invalid bucket properties: ~p", [Details]}, State}
    end.

process_stream(_, _, State) ->
    {ignore, State}.
