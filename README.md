# Cassaforte, a Clojure Cassandra Client

Cassaforte is a small, easy to use Clojure client for Apache Cassandra
(1.2+), part of the [ClojureWerkz libraries](http://clojurewerkz.org).

For quickstart, please refer to our [Getting Started with Clojure and Cassandra](http://clojurecassandra.info/articles/getting_started.html)
guide.

## Project Goals

 * Provide a Clojure-friendly, easy to use API that reflects Cassandra's data model well. Dealing with the Cassandra Thrift API quirks is counterproductive.
 * Be well maintained.
 * Be well documented.
 * Be well tested.
 * Target Cassandra 1.2 and Clojure 1.4 and later from the ground up.
 * Integrate with libraries like Joda Time.
 * Support URI connections to be friendly to Heroku and other PaaS providers.



## Project Maturity

Cassaforte is a relatively young project. It took about a year to reach `1.0`. It is used heavily in a
monitoring and event collection solution that processes fairly large
amount of data.

Cassaforte is based on the now stable new [DataStax Java driver for
Cassandra](https://github.com/datastax/java-driver) and
[Hayt](https://github.com/mpenet/hayt), a fairly battle tested CQL
generation DSL library.



## Dependency Information (Artifacts)

Cassaforte artifacts are [released to Clojars](https://clojars.org/clojurewerkz/cassaforte). If you are using Maven, add the following repository
definition to your `pom.xml`:

``` xml
<repository>
  <id>clojars.org</id>
  <url>http://clojars.org/repo</url>
</repository>
```

### The Most Recent Version

With Leiningen:

``` clojure
[clojurewerkz/cassaforte "1.3.0-beta5"]
```

With Maven:

``` xml
<dependency>
  <groupId>clojurewerkz</groupId>
  <artifactId>cassaforte</artifactId>
  <version>1.3.0-beta5</version>
</dependency>
```


## Supported Features

 * Connection to a single node or a cluster
 * _All_ CQL operations
 * CQL 3.0 queries, including queries with placeholders (?, a la JDBC)
 * Nice CQL query DSL for Clojure
 * Automatic deserialization of column names and values according to the schema



## Supported Clojure Versions

Cassaforte supports Clojure 1.4+.



## Supported Apache Cassandra Versions

Cassaforte is built from the ground up for CQL 3 and Cassandra 1.2+
and 2.x.



## Documentation & Examples

Please refer to our [Getting Started with Clojure and
Cassandra](http://clojurecassandra.info/articles/getting_started.html)
guide.  [Documentation guides](http://clojurecassandra.info) are not
finished and will be improved over time.

[API reference](http://reference.clojurecassandra.info/) is also available.


Don't hesitate to join our [mailing
list](https://groups.google.com/forum/?fromgroups#!forum/clojure-cassandra)
and ask questions, too!



## Community

To subscribe for announcements of releases, important changes and so on, please follow
[@ClojureWerkz](https://twitter.com/#!/clojurewerkz) on Twitter.


## Cassaforte Is a ClojureWerkz Project

Cassaforte is part of the [group of libraries known as ClojureWerkz](http://clojurewerkz.org), together with
[Monger](http://clojuremongodb.info), [Elastisch](http://clojureelasticsearch.info), [Langohr](http://clojurerabbitmq.info),
[Welle](http://clojureriak.info), [Titanium](http://titanium.clojurewerkz.org) and several others.



## Continuous Integration

[![Continuous Integration status](https://secure.travis-ci.org/clojurewerkz/cassaforte.png)](http://travis-ci.org/clojurewerkz/cassaforte)

CI is hosted by [travis-ci.org](http://travis-ci.org)


## Development

Cassaforte uses [Leiningen 2](https://github.com/technomancy/leiningen/blob/master/doc/TUTORIAL.md). Make
sure you have it installed and then run tests against all supported Clojure versions using

```
lein2 all test
```

Then create a branch and make your changes on it. Once you are done with your changes and all
tests pass, submit a pull request on Github.



## License

Copyright (C) 2012-2013 Michael S. Klishin, Alex Petrov

Double licensed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html) (the same as Clojure) or
the [Apache Public License 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
