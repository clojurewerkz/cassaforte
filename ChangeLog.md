## Changes between 2.0.0 and 2.1.0

### DataStax Java Driver Update

DataStax Java driver has been updated to `2.1.4`.


## Changes between 2.0.0-rc5 and 2.0.0

There were no code changes in 2.0 GA. The project
now includes Apache Software License 2.0 headers.
License files for both APL2 and EPL1 are included
in the distribution.


## Changes between 2.0.0-rc4 and 2.0.0-rc5

### Correct Deserialisation of Empty Strings

Empty string values are now correctly deserialised (previously they
were returned as `nil`).

GH issue: [#91](https://github.com/clojurewerkz/cassaforte/issues/91).


## Changes between 2.0.0-rc3 and 2.0.0-rc4

### Cassandra Native Protocol v2 as Default

To preserve Cassandra 2.0 compatibility yet continue using the most recent Cassandra Java driver
Cassaforte now uses native protocol v2 by default. v3 can be opted into
using the `:protocol-version` connection option (with value of `3`).


### Hayt 2.0

Hayt dependency has been upgraded to `2.0` (GA).


## Changes between 2.0.0-rc2 and 2.0.0-rc3

### Fetch Size Support

(Internal to the client) automatic paging of result set rows now can be configured
or disabled altogether, e.g. when running into problems similar to [CASSANDRA-6722](https://issues.apache.org/jira/browse/CASSANDRA-6722).

`clojurewerkz.cassaforte.client/with-fetch-size` is a macro that does that:

``` clojure
(require '[clojurewerkz.cassaforte.client :as cc])

;; alter page size
(cc/with-fetch-size 8192
  (comment "SELECT queries go here"))

;; disable internal client paging
(cc/with-fetch-size Integer/MAX_VALUE
  (comment "SELECT queries go here"))
```

Default fetch size is unaltered (Cassaforte relies on the Java driver default). This setting
only makes sense for a certain subset of `SELECT` queries.



## Changes between 2.0.0-rc1 and 2.0.0-rc2

### Fixes Race Condition in Async Operations

Async database operations no longer suffer from a race condition between
issueing them and definiting callbacks on the returned future value.

Contributed by Kirill Chernyshov.

### Compression Option

`:compression` is a new option that can be used when connecting:

``` clojure
(require '[clojurewerkz.cassaforte.client :as client])

(let [s (client/connect ["127.0.0.1"] "my-keyspace" {:compression :snappy})]
  )
```

Valid compression values are:

 * `:snappy`
 * `:lz4`
 * `:none` (or `nil`)

Contirbuted by Max Barnash (DataStax).


## Changes between 2.0.0-beta9 and 2.0.0-rc1

### Clojure 1.4 and 1.5 Support Dropped

Cassaforte now requires Clojure `1.6.0`.


## Changes between 2.0.0-beta8 and 2.0.0-beta9

### Collections Converted to Clojure Data Structures

Cassandra maps, sets and lists are now automatically converted to their
immutable Clojure counterparts.


### Atomic Batches Support

[Atomic batches](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/batch_r.html) are now easier to use with Cassaforte:

``` clojure
(require '[clojurewerkz.cassaforte.client :as client])
(require '[clojurewerkz.cassaforte.cql :as cql :refer :all])
(require '[clojurewerkz.cassaforte.query :refer :all])
(require '[qbits.hayt.dsl.statement :as hs])

(let [s (client/connect ["127.0.0.1"])]
  (cql/atomic-batch s (queries
                         (hs/insert :users (values {:name "Alex" :city "Munich" :age (int 19)}))
                         (hs/insert :users (values {:name "Fritz" :city "Hamburg" :age (int 28)})))))
```


## Changes between 2.0.0-beta7 and 2.0.0-beta8

`2.0.0-beta8` introduces a major **breaking API change**.

### Query DSL Taken From Hayt 2.0

Cassaforte no longer tries to support query condition DSLs for both Hayt 1.x
and Hayt 2.0. Hayt 2.0 is the only supported flavour now and
is the future.

Some examples of the changes:

``` clojure
;; before
(where :name "Alex")

;; after
(where [[= :name "Alex"]])
(where {:name "Alex"})


;; before
(where :name "Alex" :city "Munich")

;; after
(where [[= :name "Alex"]
        [= :city "Munich"]])
(where {:name "Alex" :city "Munich"})


;; before
(where :name "Alex" :age [> 25])

;; after
(where [[= :name "Alex"]
        [> :age  25]])

;; before
(where :name "Alex" :city [:in ["Munich" "Frankfurt"]])

;; after
(where [[= :name "Alex"]
        [:in :city ["Munich" "Frankfurt"]]])
```

As it's easy to see, the new condition style closer resembles
Clojure itself and thus was a reasonable decision on behalf of Hayt
developers.


## Changes between 2.0.0-beta5 and 2.0.0-beta7

### Hayt Upgraded to 2.0

[Hayt](https://github.com/mpenet/hayt) was upgraded to 2.0.


## Changes between 2.0.0-beta4 and 2.0.0-beta5

### clojurewerkz.cassandra.cql/iterate-table Now Terminates

`clojurewerkz.cassandra.cql/iterate-table` no longer produces an infinite
sequence.


### Keyspace as Option

It is now possible to choose keyspace via an option:

``` clojure
(ns cassaforte.docs
  (:require [clojurewerkz.cassaforte.client :as cc]))

(let [conn (cc/connect {:hosts ["127.0.0.1"] :keyspace "a-keyspace"})]
  )
```

Contributed by Max Barnash (DataStax).



## Changes between 2.0.0-beta3 and 2.0.0-beta4

### URI Connections

It is now possible to connect to a node and switch to a namespace
using a URI string:

``` clojure
(ns cassaforte.docs
  (:require [clojurewerkz.cassaforte.client :as cc]))

;; connects to node 127.0.0.1:9042 and uses "new_cql_keyspace" as keyspace
(cc/connect-with-uri "cql://127.0.0.1:9042/new_cql_keyspace")
```



## Changes between 2.0.0-beta2 and 2.0.0-beta3

### Cassandra 2.1 Compatibility

Cassaforte 2.0 is compatible with Cassandra 2.1.


### Prepared Statement Cache Removed

Prepared statement cache was affecting client correctness in some cases
and was removed.


### Clojure 1.7.0-alpha2+ Compatibility

Cassaforte is now compatible with Clojure `1.7.0-alpha2` and later versions.



## Changes between 1.3.0 and 2.0.0-beta2

Cassaforte 2.0 has [breaking API changes](http://blog.clojurewerkz.org/blog/2014/04/26/major-breaking-public-api-changes-coming-in-our-projects/) in most namespaces.

### Client (Session) is Explicit Argument

All Cassaforte public API functions that issue requests to Cassandra now require
a client (session) to be passed as an explicit argument:

```clj
(ns cassaforte.docs
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql    :as cql]))

(let [conn (cc/connect ["127.0.0.1"])]
  (cql/use-keyspace conn "cassaforte_keyspace"))
```

```clj
(ns cassaforte.docs
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql    :as cql]
            [clojurewerkz.cassaforte.query :refer :all]))

(let [conn (cc/connect ["127.0.0.1"])]
  (cql/create-table conn "user_posts"
                (column-definitions {:username :varchar
                                     :post_id  :varchar
                                     :body     :text
                                     :primary-key [:username :post_id]})))
```

```clj
(ns cassaforte.docs
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql    :as cql]))

(let [conn (cc/connect ["127.0.0.1"])]
  (cql/insert conn "users" {:name "Alex" :age (int 19)}))
```

### Policy Namespace

Policy-related functions from `clojurewerkz.cassaforte.client` were extracted into
`clojurewerkz.cassaforte.policies`:

```clojure
(require '[clojurewerkz.cassaforte.policies :as cp])

(cp/exponential-reconnection-policy 100 1000)
```

``` clojure
(require '[clojurewerkz.cassaforte.policies :as cp])

(let [p (cp/round-robin-policy)]
  (cp/token-aware-policy p))
```

### DataStax Java Driver Update

DataStax Java driver has been updated to `2.1.x`.


### Cassandra Sessions Compatible with with-open

`Session#shutdown` was renamed to `Session#close` in
cassandra-driver-core. Cassaforte needs to be adapted to that.

Contributed by Jarkko Mönkkönen.

### TLS and Kerberos Support

Cassaforte now supports TLS connections and Kerberos authentication
via [DataStax CQL extensions](http://www.datastax.com/dev/blog/accessing-secure-dse-clusters-with-cql-native-protocol).

The `:ssl` connection option now can be a map with two keys:

 * `:keystore-path`
 * `:keystore-password`

which provide a path and password to a [JDK KeyStore](http://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html)
on disk, created with
[keytool](http://docs.oracle.com/javase/7/docs/technotes/tools/solaris/keytool.html).

Optionally, an instance of
[SSLOptions](http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/SSLOptions.html)
can be provided via the `:ssl-options` connection option.

Contributed by Max Barnash.

GH issue: [#60](https://github.com/clojurewerkz/cassaforte/pull/60).

### Support for overriding default SSL cipher suites

Providing a `:cipher-suites` key in the `:ssl` connection option allows to specify cipher suites
that are enabled when connecting to a cluster with SSL.
The value of this key is a Seq of Strings (e.g. a vector) where each item specifies a cipher suite:

```clj
(ns cassaforte.docs
  (:require [clojurewerkz.cassaforte.client :as cc]))

(cc/build-cluster {:ssl {:keystore-path "path/to/keystore"
                         :keystore-password "password"}})]
                         :cipher-suites ["TLS_RSA_WITH_AES_128_CBC_SHA"]}}
```

The `:cipher-suites` key is optional and may be omitted, in which case Datastax Java driver's
default cipher suites (`com.datastax.driver.core.SSLOptions/DEFAULT_SSL_CIPHER_SUITES`) are enabled.

This can be used to work around the need to install Java Cryptography Extension (JCE) Unlimited Strength
Jurisdiction Policy Files required by the default set of cipher suites. `TLS_RSA_WITH_AES_128_CBC_SHA`
is a suite in the default set that works with the standard JCE. E.g. by specifying just that one,
as in the code example, the standard JCE is enough.

Contributed by Juhani Hietikko.

GH issue: [#61](https://github.com/clojurewerkz/cassaforte/pull/61).

## Changes between 1.2.0 and 1.3.0

### Clojure 1.6 By Default

The project now depends on `org.clojure/clojure` version `1.6.0`. It is
still compatible with Clojure 1.4 and if your `project.clj` depends on
a different version, it will be used, but 1.6 is the default now.

We encourage all users to upgrade to 1.6, it is a drop-in replacement
for the majority of projects out there.


### Cassandra Java Driver Update

Cassandra Java driver has been updated to `2.0.x`.


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

### Alternative `where` syntax

Now it is possible to specify hash in where clause, which makes queries 
more composable:

```clj
(select :users
        (where {:city "Munich"
                :age [> (int 5)]})
        (allow-filtering true))
```

### Batch Insert Improvements

Clauses to be specified for each record in `insert-batch`:

``` clojure
(let [input [[{:name "Alex" :city "Munich"} (using :ttl 350)]
             [{:name "Alex" :city "Munich"} (using :ttl 350)]]]
  (insert-batch th/session :users input))
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
