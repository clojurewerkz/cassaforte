(defproject clojurewerkz/cassaforte "1.0.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :description "A Clojure client for Apache Cassandra"
  :url "http://clojurecassandra.info"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure                          "1.5.1"]
                 [cc.qbits/alia                                "1.2.0"]]
  :source-paths      ["src/clojure"]
  :profiles       {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
                   :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
                   :1.6 {:dependencies [[org.clojure/clojure "1.6.0-master-SNAPSHOT"]]}
                   :master {:dependencies [[org.clojure/clojure "1.6.0-master-SNAPSHOT"]]}
                   :dev {:jvm-opts     ["-Dlog4j.configuration=log4j.properties.unit"
                                        "-Xmx2048m"
                                        "-javaagent:lib/jamm-0.2.5.jar"]
                         :resource-paths ["resources"]
                         :dependencies [[org.xerial.snappy/snappy-java      "1.0.5"]
                                        [commons-lang/commons-lang          "2.6"]
                                        [org.apache.cassandra/cassandra-all "1.2.4"]
                                        [org.clojure/tools.trace            "0.7.5"]
                                        ]}}
  :aliases        {"all" ["with-profile" "dev:dev,1.3:dev,1.4:dev,1.6:dev,master"]}
  :test-selectors {:focus   :focus
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
  :warn-on-reflection true
  :pedantic :warn)
