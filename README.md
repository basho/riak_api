# `riak_api` - Riak Client APIs

This OTP application encapsulates services for presenting Riak's
public-facing interfaces. Currently this means a generic interface for
exposing Protocol Buffers-based services; HTTP services via Webmachine
will be moved here at a later time.

## Contributing

We encourage contributions to `riak_api` from the community.

1. Fork the [`riak_api`](https://github.com/basho/riak_api) repository
   on Github.
2. Clone your fork or add the remote if you already have a clone of
   the repository.
   ```shell
   git clone git@github.com:yourusername/riak_kv.git
   # or
   git remote add mine git@github.com:yourusername/riak_kv.git
   ```
3. Create a topic branch for your change.
   ```shell
   git checkout -b some-topic-branch
   ```
4. Make your change and commit. Use a clear and descriptive commit
   message, spanning multiple lines if detailed explanation is needed.
5. Push to your fork of the repository and then send a pull-request
   through Github.
   ```shell
   git push mine some-topic-branch
   ```
6. A Basho engineer or community maintainer will review your patch and
   merge it into the main repository or send you feedback.
