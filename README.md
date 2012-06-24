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
 * Learn from other clients like the Java and Ruby ones.


## Supported Features

Nothing so far.


## Supported Clojure versions

Cassaforte is built from the ground up for Clojure 1.3 and up.


## Documentation & Examples

Cassaforte is a young project and until 1.0 is released and documentation guides are written,
it may be challenging to use for anyone except the author. For code examples, see our test
suite.

Once documentation site is up, we will update this document.


## Community

To subscribe for announcements of releases, important changes and so on, please follow
[@ClojureWerkz](https://twitter.com/#!/clojurewerkz) on Twitter.



## Maven Artifacts

### Snapshots

With Leiningen:

    [clojurewerkz/cassaforte "1.0.0-SNAPSHOT"]


With Maven:

    <dependency>
      <groupId>clojurewerkz</groupId>
      <artifactId>cassaforte</artifactId>
      <version>1.0.0-SNAPSHOT</version>
    </dependency>



## This is a Work In Progress

Cassaforte is very much a work in progress and right now, please keep this in mind.


## Cassaforte Is a ClojureWerkz Project

Cassaforte is part of the [group of libraries known as ClojureWerkz](http://clojurewerkz.org), together with
[Monger](https://github.com/michaelklishin/monger), [Welle](https://github.com/michaelklishin/welle), [Elastisch](https://github.com/clojurewerkz/elastisch), [Neocons](https://github.com/michaelklishin/neocons) and several others.



## Continuous Integration

[![Continuous Integration status](https://secure.travis-ci.org/clojurewerkz/cassaforte.png)](http://travis-ci.org/clojurewerkz/cassaforte)

CI is hosted by [travis-ci.org](http://travis-ci.org)


## Development

Cassaforte uses [Leiningen 2](https://github.com/technomancy/leiningen/blob/master/doc/TUTORIAL.md). Make
sure you have it installed and then run tests against all supported Clojure versions using

    lein2 ci test

Then create a branch and make your changes on it. Once you are done with your changes and all
tests pass, submit a pull request on Github.


## License

Copyright (C) 2012 Michael S. Klishin, Alex Petrov

Distributed under the Eclipse Public License, the same as Clojure.
