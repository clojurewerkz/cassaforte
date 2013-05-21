# Cassaforte

Cassaforte is an young idiomatic Clojure client for Cassandra.
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

We use Cassaforte heavily for our monitoring solution, that processes fairly large amount of data.
Until DataStax makes a stable, non RC release of java-driver that we use underneath, we can't make a
final release. Project is known to behave well, it's tested and used in production.



## Supported Features

 * Connection to a single node and cluster
 * _All_ CQL operations
 * CQL 3.0 queries, including queries with placeholders (?, a la JDBC)
 * Deserialization of column names and values according to response schema



## Supported Clojure versions

Cassaforte is built from the ground up for Clojure 1.4 and up.



## Supported Apache Cassandra versions

Cassaforte is built from the ground up for Cassandra 1.2 and up and is built around CQL 3.



## Documentation & Examples

Please refer to our [Getting Started with Clojure and Cassandra](http://clojurecassandra.info/articles/getting_started.html) guide.
Don't hesitate to join our [mailing list](https://groups.google.com/forum/?fromgroups#!forum/clojure-cassandra) and ask questions, too!



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

```clojure
[clojurewerkz/cassaforte "1.0.0-rc3"]
```

With Maven:

```xml
<dependency>
  <groupId>clojurewerkz</groupId>
  <artifactId>cassaforte</artifactId>
  <version>1.0.0-rc3</version>
</dependency>
```



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

```
lein2 all test
```

Then create a branch and make your changes on it. Once you are done with your changes and all
tests pass, submit a pull request on Github.



## License

Copyright (C) 2012-2013 Michael S. Klishin, Alex Petrov

Distributed under the Eclipse Public License, the same as Clojure.
