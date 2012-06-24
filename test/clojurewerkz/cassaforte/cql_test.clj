(ns clojurewerkz.cassaforte.cql-test
  (require [clojurewerkz.cassaforte.client :as cc]
           [clojurewerkz.cassaforte.cql    :as cql])
  (:use clojure.test
        clojurewerkz.cassaforte.conversion))

(cc/connect! "127.0.0.1" "CassaforteTest1")


(deftest ^{:cql true} test-create-and-drop-column-family-using-cql
  (let [query  "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name));"
        result (cql/execute-raw query)]
    (is (cql/void-result? result))
    (is (nil? (.getRows result)))
    (cql/execute-raw "DROP COLUMNFAMILY libraries;")))
