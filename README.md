# Cassaforte

Cassaforte is a young idiomatic Clojure client for Apache Cassandra (1.2+).
Its API and code style closely follow other ClojureWerkz projects, namely [Monger](https://clojuremongodb.info), [Welle](https://clojureriak.info),
[Neocons](https://clojureneo4j.info), [Elastisch](https://clojureelasticsearch.info) and [Spyglass](https://clojurememcached.info).


## Project Goals

 * Provide a Clojure-friendly, easy to use API that reflects Cassandra's data model well. Dealing with the Cassandra Thrift API quirks is counterproductive.
 * Be well maintained.
 * Be well documented.
 * Be well tested.
 * Target Cassandra 1.2 and Clojure 1.4 and later from the ground up.
 * Integrate with libraries like clojure.data.json and Joda Time.
 * Support URI connections to be friendly to Heroku and other PaaS providers.


## Project Maturity

Cassaforte is *young*. It is getting to the point where we can confidently
recommend it to other developers, but not exactly there yet. We will update
this document when it's ready for prime time.



## Supported Features

 * Connection to a single node
 * Create, destroy keyspace
 * CQL 3.0 queries, including queries with placeholders (?, a la JDBC)
 * Deserialization of column names and values according to response schema (not all types are supported yet)


## Supported Clojure versions

Cassaforte is built from the ground up for Clojure 1.3 and up.


## Supported Apache Cassandra versions

Cassaforte is built from the ground up for Cassandra 1.2 and up and is built around CQL 3.



## Documentation & Examples

Cassaforte is a young project and until 1.0 is released and documentation guides are written,
it may be challenging to use for anyone except the author. For code examples, see our test
suite.

Once the documentation site is up, we will update this section.


## Community

To subscribe for announcements of releases, important changes and so on, please follow
[@ClojureWerkz](https://twitter.com/#!/clojurewerkz) on Twitter.



## Maven Artifacts

Cassaforte artifacts are [released to Clojars](https://clojars.org/clojurewerkz/cassaforte). If you are using Maven, add the following repository
definition to your `pom.xml`:

``` xml
<repository>
  <id>clojars.org</id>
  <url>http://clojars.org/repo</url>
</repository>
```

### Most recent pre-release version

With Leiningen:

    [clojurewerkz/cassaforte "1.0.0-beta1"]


With Maven:

    <dependency>
      <groupId>clojurewerkz</groupId>
      <artifactId>cassaforte</artifactId>
      <version>1.0.0-beta1</version>
    </dependency>


## Cassaforte Is a ClojureWerkz Project

Cassaforte is part of the [group of libraries known as ClojureWerkz](http://clojurewerkz.org), together with
[Monger](https://clojuremongodb.info), [Welle](https://clojureriak.info), [Neocons](https://clojureneo4j.info),
[Elastisch](https://clojureelasticsearch.info) and several others.



## Continuous Integration

[![Continuous Integration status](https://secure.travis-ci.org/clojurewerkz/cassaforte.png)](http://travis-ci.org/clojurewerkz/cassaforte)

CI is hosted by [travis-ci.org](http://travis-ci.org)


## Development

Cassaforte uses [Leiningen 2](https://github.com/technomancy/leiningen/blob/master/doc/TUTORIAL.md). Make
sure you have it installed and then run tests against all supported Clojure versions using

    lein2 all test

Then create a branch and make your changes on it. Once you are done with your changes and all
tests pass, submit a pull request on Github.



## License

Copyright (C) 2012-2013 Michael S. Klishin, Alex Petrov

Double licensed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html) (the same as Clojure) or
the [Apache Public License 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
