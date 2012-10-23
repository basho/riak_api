-define(ETS_NAME, riak_api_pb_registrations).
-define(ETS_HEIR, {heir, self(), undefined}).
-define(ETS_OPTS, [protected,
                   named_table,
                   set,
                   ?ETS_HEIR,
                   {read_concurrency, true}]).
