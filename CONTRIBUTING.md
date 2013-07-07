## Pre-requisites

The project uses [Leiningen 2](https://leiningen.org) and **does not** require Cassandra to be running
locally (an embedded node is used for tests).

Make sure you have Leiningen 2 installed and then run tests against all supported Clojure versions using

    lein2 all do clean, javac, test

## Pull Requests

Then create a branch and make your changes on it. Once you are done with your changes and all
tests pass, write a [good, detailed commit message](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html) submit a pull request on GitHub.
