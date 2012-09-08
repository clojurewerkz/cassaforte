(defproject clojurewerkz/cassaforte "1.0.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :description "A Clojure client for Apache Cassandra"
  :url "http://github.com/clojurewerkz/cassaforte"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure                   "1.4.0"]
                 [org.apache.cassandra/cassandra-all    "1.1.4"]
                 [org.apache.cassandra/cassandra-thrift "1.1.4"]
                 [clojurewerkz/support                  "0.7.0"]
                 [clj-time                              "0.4.4"]]
  :source-paths      ["src/clojure"]
  :java-source-paths ["src/java"]
  :javac-options     ["-target" "1.6" "-source" "1.6"]
  :profiles       {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
                   :1.5 {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}}
  :aliases        {"all" ["with-profile" "dev:dev,1.3:dev,1.5"]}
  :test-selectors {:focus   :focus
                   :cql     :cql
                   :schema  :schema
                   :indexes :indexes
                   :default (constantly true)
                   :ci (complement :skip-ci)}
  :repositories   {"sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                               :snapshots false
                               :releases {:checksum :fail :update :always}}
                   "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                               :snapshots true
                               :releases {:checksum :fail :update :always}}}
  :warn-on-reflection true)
