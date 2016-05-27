(defproject clojurewerkz/cassaforte "3.0.0-alpha2-SNAPSHOT"
  :min-lein-version  "2.5.1"
  :description       "A Clojure client for Apache Cassandra"
  :url               "http://clojurecassandra.info"
  :license           {:name "Eclipse Public License"
                      :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies      [[org.clojure/clojure                          "1.8.0"]
                      [com.datastax.cassandra/cassandra-driver-core "3.0.2"]
                      [com.datastax.cassandra/cassandra-driver-dse  "3.0.0-rc1"]
                      [org.clojure/core.match                       "0.3.0-alpha4"]]
  :aot [clojurewerkz.cassaforte.query]
  :source-paths      ["src/clojure"]
  :test-paths        ["test/clojure" "test/java"]
  :java-source-paths ["test/java" "src/java"]
  :profiles          {:1.6    {:dependencies [[org.clojure/clojure "1.6.0"]]}
                      :1.7    {:dependencies [[org.clojure/clojure "1.7.0"]]}
                      :master {:dependencies [[org.clojure/clojure "1.9.0-master-SNAPSHOT"]]}
                      :dev    {:jvm-opts       ["-Dlog4j.configuration=log4j.properties.unit"
                                                "-Xmx2048m"
                                                "-javaagent:lib/jamm-0.2.5.jar"]
                               :resource-paths ["resources"]

                               :plugins        [[codox           "0.8.10"]
                                                [jonase/eastwood "0.2.1"]]

                               :dependencies   [[com.codahale.metrics/metrics-core "3.0.2"]
                                                [org.xerial.snappy/snappy-java     "1.1.1.6"]
                                                [org.clojure/tools.trace           "0.7.8"]
                                                [clj-time                          "0.9.0"]

                                                ;; test/development
                                                [org.clojure/tools.namespace "0.2.10"]
                                                [org.clojure/test.check      "0.7.0"]
                                                [com.gfredericks/test.chuck  "0.1.17"]
                                                ]}}
  :aliases           {"all" ["with-profile" "dev:dev,1.6:dev,1.7:dev,master"]}
  :test-selectors    {:focus   :focus
                      :client  :client
                      :cql     :cql
                      :schema  :schema
                      :stress  :stress
                      :indexes :indexes
                      :default (fn [m] (not (:stress m)))
                      :ci      (complement :skip-ci)}
  :repositories      {"sonatype" {:url       "http://oss.sonatype.org/content/repositories/releases"
                                  :snapshots false
                                  :releases  {:checksum :fail :update :always}}
                      "sonatype-snapshots" {:url       "http://oss.sonatype.org/content/repositories/snapshots"
                                            :snapshots true
                                            :releases  {:checksum :fail :update :always}}}
  :global-vars       {*warn-on-reflection* true}
  :pedantic          :warn
  :codox             {:src-dir-uri               "https://github.com/clojurewerkz/cassaforte/blob/master/"
                      :sources                   ["src/clojure/"]
                      :src-linenum-anchor-prefix "L"
                      :exclude                   [clojurewerkz.cassaforte.conversion
                                                  clojurewerkz.cassaforte.aliases
                                                  clojurewerkz.cassaforte.metrics
                                                  clojurewerkz.cassaforte.debug
                                                  clojurewerkz.cassaforte.bytes]
                      :output-dir                "doc/api"}
)
