# Cellar

An experimental KV store, which implements an LSM on top of [Bolt](https://github.com/boltdb/bolt) segments.

## Status

**EXPERIMENTAL** – the API is evolving and the implementation is new

[![Build Status](https://travis-ci.org/couchbaselabs/cellar.svg?branch=master)](https://travis-ci.org/couchbaselabs/cellar)
[![Coverage Status](https://coveralls.io/repos/github/couchbaselabs/cellar/badge.svg?branch=master)](https://coveralls.io/github/couchbaselabs/cellar?branch=master)
[![GoDoc](https://godoc.org/github.com/couchbaselabs/cellar?status.svg)](https://godoc.org/github.com/couchbaselabs/cellar)
[![codebeat badge](https://codebeat.co/badges/cb2bcac1-10c9-492a-bffe-abd0e179a16b)](https://codebeat.co/projects/github-com-couchbaselabs-cellar)
[![Go Report Card](https://goreportcard.com/badge/github.com/couchbaselabs/cellar)](https://goreportcard.com/report/github.com/couchbaselabs/cellar)

## High Level Concept

- Data coming into Cellar is batched.  Each batch is written out to its own Bolt segment.
- Reads from Cellar must navigate all the live Bolt segments.
- Over time, you have too many segments, and must merge segments.

**NOTE**: if you can arrange to write all keys in a batch in sorted order, we can take advantage of Bolt's strengths.  One way to build batches and write them to Cellar in sorted order is to place [moss](https://github.com/couchbase/moss) in front of Cellar.  In the future we may offer another package to combine these two projects seamlessly.

## Features

- API inspired by Bolt
- But, only 1 bucket.  Support for nested or multiple buckets was removed.
- Configurable merge policies.  Currently only one really dumb implementation.

## Performance

Is this actually faster for any use cases?

We don't know yet.  This is an ongoing experiment.

## License

Apache 2.0
