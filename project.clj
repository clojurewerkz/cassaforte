(defproject clojurewerkz/cassaforte "2.1.0-SNAPSHOT"
  :min-lein-version "2.5.0"
  :description "A Clojure client for Apache Cassandra"
  :url "http://clojurecassandra.info"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure                          "1.6.0"]
                 [cc.qbits/hayt                                "2.0.0"]
                 [com.datastax.cassandra/cassandra-driver-core "2.1.4"]
                 [com.datastax.cassandra/cassandra-driver-dse  "2.1.4"]]
  :source-paths      ["src/clojure"]
  :java-source-paths ["src/java"]
  :profiles       {:1.7 {:dependencies [[org.clojure/clojure "1.7.0-alpha4"]]}
                   :master {:dependencies [[org.clojure/clojure "1.7.0-master-SNAPSHOT"]]}
                   :dev {:jvm-opts     ["-Dlog4j.configuration=log4j.properties.unit"
                                        "-Xmx2048m"
                                        "-javaagent:lib/jamm-0.2.5.jar"]
                         :resource-paths ["resources"]
                         :plugins [[codox "0.8.10"]]
                         :dependencies [[com.codahale.metrics/metrics-core "3.0.2"]
                                        [org.xerial.snappy/snappy-java     "1.1.1.6"]
                                        [org.clojure/tools.trace           "0.7.8"]
                                        [clj-time                          "0.9.0-beta1"]]}}
  :aliases        {"all" ["with-profile" "dev:dev,1.7:dev,master"]}
  :test-selectors {:focus   :focus
                   :client  :client
                   :cql     :cql
                   :schema  :schema
                   :stress  :stress
                   :indexes :indexes
                   :default (fn [m] (not (:stress m)))
                   :ci      (complement :skip-ci)}
  :repositories {"sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                             :snapshots false
                             :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}}
  :global-vars {*warn-on-reflection* true}
  :pedantic :warn
  :codox {:src-dir-uri "https://github.com/clojurewerkz/cassaforte/blob/master/"
          :sources ["src/clojure/"]
          :src-linenum-anchor-prefix "L"
          :exclude [clojurewerkz.cassaforte.conversion
                    clojurewerkz.cassaforte.aliases
                    clojurewerkz.cassaforte.metrics
                    clojurewerkz.cassaforte.debug
                    clojurewerkz.cassaforte.bytes]
          :output-dir "doc/api"})
