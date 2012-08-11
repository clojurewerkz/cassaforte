(ns clojurewerkz.cassaforte.schema-test
  (require [clojurewerkz.cassaforte.client :as cc]
           [clojurewerkz.cassaforte.schema :as sch]
           [clojurewerkz.cassaforte.cql    :as cql])
  (:use clojure.test
        clojurewerkz.cassaforte.conversion
        clojurewerkz.cassaforte.test.helper))

(cc/connect! "127.0.0.1" "CassaforteTest1")

(deftest ^{:schema true} test-describe-keyspace-raw
  (let [keyspace "CassaforteTest1"
        ks-def   (sch/describe-keyspace-raw keyspace)]
    (is (= (.getName ks-def) keyspace))))


(deftest ^{:schema true} test-add-and-drop-keyspace
  (let [keyspace       "CassaforteTest2"
        strategy-class "org.apache.cassandra.locator.SimpleStrategy"
        strategy-opts  {:replication_factor "1"}
        cf-defs        [(build-column-family-definition keyspace "movies")]]
    (is (sch/add-keyspace keyspace strategy-class cf-defs :strategy-opts strategy-opts))
    (is (sch/drop-keyspace keyspace))))


(deftest ^{:schema true :indexes true} test-create-and-drop-an-index-using-a-convenience-function
  (with-thrift-exception-handling
    (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                                   language varchar,
                                                   PRIMARY KEY (name))")
    (is (cql/void-result? (sch/create-index "libraries" "language")))
    (is (cql/void-result? (sch/drop-index "libraries" "language")))
    (is (cql/void-result? (sch/create-index "libraries" "language" "by_language")))
    (is (cql/void-result? (sch/drop-index "by_language")))
    (cql/execute "DROP COLUMNFAMILY ?" ["libraries"])))
