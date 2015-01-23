%% --------------------------------------------------------------------------
%%
%% riak_core_pb_bucket_key_apl: Expose Core active preflist functionality to
%%                              Protocol Buffers
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%% --------------------------------------------------------------------------

%% @doc <p>The Bucket-Key Preflist (Primaries & Fallbacks) PB service
%% for Riak Core. This service covers the following request messages in the
%% original protocol:</p>
%%
%% <pre>
%% 33 - RpbGetBucketKeyActivePreflistReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%% 34 - RpbGetBucketKeyActivePreflistResp
%% </pre>
%%
%% @end
-module(riak_core_pb_bucket_key_apl).

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
decode(Code, Bin) when Code == 33 ->
    Msg =  riak_pb_codec:decode(Code, Bin),
    case Msg of
        #rpbgetbucketkeyactivepreflistreq{type =T, bucket =B, key =Key} ->
            Bucket = riak_core_pb_bucket:bucket_type(T, B),
            {ok, Msg, {"riak_core.get_active_preflist", {Bucket, Key}}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% Get bucket-key preflist primaries
process(#rpbgetbucketkeyactivepreflistreq{type=T, bucket=B, key =Key}, State) ->
    Bucket = riak_core_pb_bucket:maybe_create_bucket_type(T, B),
    Preflist = riak_core_apl:get_apl_ann_with_pnum({Bucket, Key}),
    Message = riak_pb_codec:encode_apl_ann(Preflist),
    {reply, Message, State}.

process_stream(_, _, State) ->
    {ignore, State}.
