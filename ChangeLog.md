## Changes between 1.0.0-rc5 and 1.0.0-rc6

### Raw Query Execution Improvements

Raw (string) query execution is now easier to do. Low-level ops are now more explicit and easy to
use.

Update Hayt to latest version (1.1.2), allow depending on Clojure starting with 1.4.

### Functions Renamed

`clojurewerkz.cassaforte.client/render-query` is renamed to `clojurewerkz.cassaforte.client/render`.

`clojurewerkz.cassaforte.client/compile-query` is renamed to `clojurewerkz.cassaforte.client/compile`.



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
