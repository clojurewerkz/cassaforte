(ns clojurewerkz.cassaforte.cql-test
  (require [clojurewerkz.cassaforte.client :as cc]
           [clojurewerkz.cassaforte.cql    :as cql])
  (:use clojure.test
        clojurewerkz.cassaforte.conversion
        clojurewerkz.cassaforte.test.helper))

(cc/connect! "127.0.0.1" "CassaforteTest1")


;;
;; CREATE KEYSPACE, DROP KEYSPACE
;;

(deftest ^{:cql true} test-create-and-drop-keyspace-using-raw-cql
  (let [query "CREATE KEYSPACE amazeballs WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1"]
    (is (cql/void-result? (cql/execute-raw query)))
    (cql/execute-raw "DROP KEYSPACE \"amazeballs\";")))


;;
;; CREATE CF, DROP CF
;;

(deftest ^{:cql true} test-create-and-drop-column-family-using-raw-cql
  (let [query  "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name));"
        result (cql/execute-raw query)]
    (is (cql/void-result? result))
    (is (nil? (.getRows result)))
    (cql/execute-raw "DROP COLUMNFAMILY libraries;")))


;;
;; CREATE INDEX, DROP INDEX
;;

(deftest ^{:cql true} test-create-and-drop-an-index-using-raw-cql
  (with-thrift-exception-handling
    (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name))")
    (let [query  "CREATE INDEX ON libraries (language)"
          result (cql/execute-raw query)]
      (is (cql/void-result? result))
      (cql/execute-raw "DROP COLUMNFAMILY libraries;"))))


;;
;; INSERT
;;

;; TBD


;;
;; BATCH
;;

;; TBD


;;
;; INSERT with placeholders
;;

;; TBD


;;
;; INSERT with a map
;;

;; TBD


;;
;; UPDATE with placeholders
;;

;; TBD


;;
;; DELETE with placeholders
;;

;; TBD


;;
;; DELETE row
;;

;; TBD


;;
;; Raw SELECT
;;

;; TBD


;;
;; SELECT with placeholders
;;

;; TBD


;;
;; TRUNCATE
;;

;; TBD




;;
;; Conversion to CQL values, escaping
;;

(deftest ^{:cql true} test-conversion-to-cql-values
  (are [val cql] (is (= cql (cql/to-cql-value val)))
    nil "null"
    10  "10"
    10N "10"
    :age "age"))

(deftest ^{:cql true} test-keyspace-name-quoting
  (are [unquoted quoted] (is (= quoted (cql/quote-identifier unquoted)))
       "accounts" "\"accounts\""))


;;
;; Interpolation
;;

#_ (deftest ^{:cql true} test-interpolating-cql-strings-with-long-parameters
  (let [query    "SELECT * FROM people WHERE age = ?;"
        expected "SELECT * FROM people WHERE age = 27;"]
    (is (= expected (cql/interpolate-vals query 27)))))
