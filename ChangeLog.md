## Changes between 1.0.0-rc2 and 1.0.0-rc3

  * Update Hayt to latest version (1.0.0)
  * Update Cassandra to latest version (1.2.4)
  * Update java-driver to latest version (1.0.0)
  * Get rid of reflection warnings
  * Add more options for inspecing cluster
  * Improve debug output

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
