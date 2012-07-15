# Cassaforte

Cassaforte is an young idiomatic Clojure client for Cassandra.
Its API and code style closely follow other ClojureWerkz projects, namely [Monger](https://github.com/michaelklishin/monger), [Welle](https://github.com/michaelklishin/welle),
[Neocons](https://github.com/michaelklishin/neocons), [Elastisch](https://github.com/clojurewerkz/elastisch) and [Spyglass](https://github.com/clojurewerkz/spyglass).


## Project Goals

 * Provide a Clojure-friendly, easy to use API that reflects Cassandra's data model well. Dealing with the Cassandra Thrift API quirks is counterproductive.
 * Be well maintained.
 * Be well documented.
 * Be well tested.
 * Target Clojure 1.3.0 and later from the ground up.
 * Integrate with libraries like clojure.data.json and Joda Time.
 * Support URI connections to be friendly to Heroku and other PaaS providers.


## Project Maturity

Cassaforte is *very young*. We put it out there mostly to gather feedback from our friends who use Cassandra and to have CI on travis-ci.org.
It almost certainly is not useable enough for anyone but the author. When Cassaforte matures, we will update this section.



## Supported Features

 * Connection to a single node
 * CQL queries
 * Keyspace operations: create, drop, describe


## Supported Clojure versions

Cassaforte is built from the ground up for Clojure 1.3 and up.


## Supported Apache Cassandra versions

Cassaforte is built from the ground up for Cassandra 1.1 and up and is built around CQL.



## Documentation & Examples

Cassaforte is a young project and until 1.0 is released and documentation guides are written,
it may be challenging to use for anyone except the author. For code examples, see our test
suite.

Once the documentation site is up, we will update this section.


## Community

To subscribe for announcements of releases, important changes and so on, please follow
[@ClojureWerkz](https://twitter.com/#!/clojurewerkz) on Twitter.



## Maven Artifacts

Monger artifacts are [released to Clojars](https://clojars.org/clojurewerkz/cassaforte). If you are using Maven, add the following repository
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
[Monger](https://github.com/michaelklishin/monger), [Welle](https://github.com/michaelklishin/welle), [Elastisch](https://github.com/clojurewerkz/elastisch), [Neocons](https://github.com/michaelklishin/neocons) and several others.



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

Copyright (C) 2012 Michael S. Klishin, Alex Petrov

Distributed under the Eclipse Public License, the same as Clojure.
