(defproject clojurewerkz/cassaforte "1.0.0-beta12-SNAPSHOT"
  :min-lein-version "2.0.0"
  :description "A Clojure client for Apache Cassandra"
  :url "http://github.com/clojurewerkz/cassaforte"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure                   "1.4.0"]
                 [cc.qbits/hayt                         "0.1.1"]
                 [org.apache.cassandra/cassandra-all    "1.2.0"]
                 [org.apache.cassandra/cassandra-thrift "1.2.0"]
                 [org.apache.thrift/libthrift           "0.7.0"]
                 [io.netty/netty                        "3.6.2.Final"]
                 [org.slf4j/slf4j-log4j12               "1.6.6"]

                 [com.datastax.cassandra/cassandra-driver-core "1.0.0-beta1"]]
  :source-paths      ["src/clojure"]
  :java-source-paths ["src/java"]
  :javac-options     ["-target" "1.6" "-source" "1.6"]
  :profiles       {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
                   :1.5 {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
                   :dev {:dependencies [;; [com.datastax.cassandra/cassandra-driver-core "0.1.0-SNAPSHOT"]
                                        [org.xerial.snappy/snappy-java "1.0.5-SNAPSHOT"]]}}
  :aliases        {"all" ["with-profile" "dev:dev,1.3:dev,1.5"]}
  :test-selectors {:focus   :focus
                   :cql     :cql
                   :schema  :schema
                   :indexes :indexes
                   :ci (complement :skip-ci)}
  :repositories {"sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                             :snapshots false
                             :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}}
  :warn-on-reflection true
  :pedantic :warn)
