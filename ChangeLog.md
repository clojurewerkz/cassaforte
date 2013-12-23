## Changes between 1.2.0 and 1.3.0

### Cassandra Dependency

Cassaforte now does not depend on `org.apache.cassandra/cassandra-all`.
This dependency is dropped.

### Cassandra Java Driver Update

Cassandra Java driver has been updated to `2.0.0-rc2`.

### UUID Generation Helpers

`clojurewerkz.cassaforte.uuids` is a new namespace that provides UUID
generation helpers:

``` clojure
(require '[clojurewerkz.cassaforte.uuids :as uuids])

(uuids/random)
;= #uuid "d43fdc16-a9c3-4d0f-8809-512115289537"

(uuids/time-based)
;= #uuid "90cf6f40-4584-11e3-90c2-65c7571b1a52"

(uuids/unix-timestamp (uuids/time-based))
;= 1383592179743

(u/start-of (u/unix-timestamp (u/time-based)))
;= #uuid "ad1fd130-4584-11e3-8080-808080808080"

(u/end-of (u/unix-timestamp (u/time-based)))
;= #uuid "b31abb3f-4584-11e3-7f7f-7f7f7f7f7f7f"
```


### Hayt Update

Hayt dependency has been updated to `1.4.1`, which supports
`if-not-exists` in `create-keyspace`:

``` clojure
(create-keyspace "main"
	         (if-not-exists)
	         (with {:replication
	                  {:class "SimpleStrategy"
	                   :replication_factor 1 }}))
```

### Extra Clauses Support in `insert-batch`

It is now possible to use extra CQL clauses for every statement
in a batch insert (e.g. to specify TTL):

``` clojure
(cql/insert-batch "table"
  {:something "cats"}
  [{:something "dogs"} (using :ttl 60)])
```

Contributed by Sam Neubardt.



## Changes between 1.1.0 and 1.2.0

### Cassandra Java Driver Update

Cassandra Java driver has been updated to `1.0.3` which
supports Cassandra 2.0.

### Fix problem with batched prepared statements

`insert-batch` didn't play well with prepared statements, problem fixed now. You can use `insert-batch`
normally with prepared statements.

### Hayt query generator update

Hayt is updated to 1.1.3 version, which contains fixes for token function and some internal improvements
that do not influence any APIs.

### Added new Consistency level DSL

Consistency level can now be (also) passed as a symbol, without resolving it to ConsistencyLevel instance:

```clojure
(client/with-consistency-level :quorum
       (insert :users r))
```

Please note that old DSL still works and is supported.

### Password authentication supported

Password authentication is now supported via the `:credentials` option to `client/build-cluster`.
Give it a map with username and password:

```clojure
(client/build-cluster {:contact-points ["127.0.0.1"]
                       :credentials {:username "ceilingcat" :password "ohai"}
                       ;; ...
```

Query DSL added for managing users `create-user`, `alter-user`, `drop-user`, `grant`, `revoke`,
`list-users`, `list-permissions` for both multi and regular sessions.

## Changes between 1.0.0 and 1.1.0

### Fixes for prepared queries with multi-cql

Multi-cql didn't work with unforced prepared statements, now it's possible to use
`client/prepared` with multi-cql as well.

### Fixes for compound keys in iterate-world queries

Iterate world didn't work fro tables with compound primary keys. Now it's possible
to iterate over collections that have compound keys.

### Fixes for AOT Compilation

Cassaforte now can be AOT compiled: `clojurewerkz.cassaforte.client/compile`
is renamed back to `clojurewerkz.cassaforte.client/compile-query`.


## Changes between 1.0.0-rc5 and 1.0.0-rc6

### Raw Query Execution Improvements

Raw (string) query execution is now easier to do. Low-level ops are now more explicit and easy to
use.

### Underlying Java Driver update

[Java Driver](https://github.com/datastax/java-driver) was updated to latest stable version, 1.0.1.

### Support of Clojure 1.4+

Hayt was been updated to [latest version (1.1.2)](https://github.com/mpenet/hayt/commit/c4ec6d8bea49843aaf0afa22a2cc09eff6fbc866),
which allows Cassaforte support Clojure 1.4.


## Changes between 1.0.0-rc4 and 1.0.0-rc5

`cassaforte.multi.cql` is a new namespace with functions that are very similar to those
in the `cassaforte.cqll` namespace but always take a database reference as an explicit argument.

They are supposed to be used in cases when Cassaforte's "main" API that uses an implicit var is not
enough.


## Changes between 1.0.0-rc3 and 1.0.0-rc4

`1.0.0-rc4` has **breaking API changes**

### Dependency Alia is Dropped

Cassaforte no longer depends on Alia.

### Per-Statement Retry Policy and Consistency Level Settings

`clojurewerkz.cassaforte.client/prepare` now accepts two more options:

 * `consistency-level`
 * `retry-policy`

### Namespace Reshuffling

`clojurewerkz.cassaforte.cql/execute` is now `clojurewerkz.cassaforte.client/execute`,
a few less frequently used functions were also moved between namespaces.


## Changes between 1.0.0-rc2 and 1.0.0-rc3

  * Update Hayt to latest version (1.0.0)
  * Update Cassandra to latest version (1.2.4)
  * Update java-driver to latest version (1.0.0)
  * Get rid of reflection warnings
  * Add more options for inspecing cluster
  * Improve debug output
  * Add helpers for wide columns
  * Simplify deserializations, remove serializaitons since they're handled by driver internally

## Changes between 1.0.0-beta1 and 1.0.0-rc2

  * Thrift support is discontinued
  * Use [Hayt](https://github.com/mpenet/hayt) for CQL generation
  * Update to java-driver 1.0.0-rc2
  * Significantly improved test coverage
  * Major changes in CQL API, vast majority of queries won't work anymore
  * Embedded server is used for tests and has it's own API now

## 1.0.0-beta1

Initial release

Supported features:

 * Connection to a single node
 * Create, destroy keyspace
 * CQL 3.0 queries, including queries with placeholders (?, a la JDBC)
 * Deserialization of column names and values according to response schema (not all types are supported yet)
