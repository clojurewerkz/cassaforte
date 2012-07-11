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
  (with-thrift-exception-handling
    (let [query "CREATE KEYSPACE amazeballs WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1"]
    (is (cql/void-result? (cql/execute-raw query)))
    (cql/execute-raw "DROP KEYSPACE \"amazeballs\";"))))


;;
;; CREATE CF, DROP CF
;;

(deftest ^{:cql true} test-create-and-drop-column-family-using-cql
  (with-thrift-exception-handling
    (let [query  "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name));"
        result (cql/execute-raw query)]
    (is (cql/void-result? result))
    (is (empty? (:rows result)))
    (cql/execute "DROP COLUMNFAMILY ?" ["libraries"]))))

(deftest ^{:cql true} test-create-truncate-and-drop-column-family-using-cql
  (with-thrift-exception-handling
    (let [query  "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name));"
        result (cql/execute-raw query)]
    (is (cql/void-result? result))
    (is (cql/void-result? (cql/truncate "libraries")))
    (cql/execute "DROP COLUMNFAMILY ?" ["libraries"]))))


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

(deftest ^{:cql true} test-insert-and-select-count-using-raw-cql
  (with-thrift-exception-handling
    (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name))")
    (is (cql/void-result? (cql/execute-raw "INSERT INTO libraries (name, language) VALUES ('Cassaforte', 'Clojure') USING CONSISTENCY LOCAL_QUORUM AND TTL 86400")))
    (cql/execute-raw "TRUNCATE libraries;")
    (cql/execute-raw "DROP COLUMNFAMILY libraries;")))


(deftest ^{:cql true} test-insert-and-select-count-using-prepared-cql-statement
  (with-thrift-exception-handling
    (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name))")
    (is (cql/void-result? (cql/execute "INSERT INTO libraries (name, language) VALUES ('?', '?') USING CONSISTENCY LOCAL_QUORUM AND TTL 86400" ["Cassaforte", "Clojure"])))
    (let [res (cql/execute "SELECT COUNT(*) FROM libraries")]
      (is (cql/rows-result? res)))
    (cql/execute-raw "TRUNCATE libraries;")
    (cql/execute-raw "DROP COLUMNFAMILY libraries;")))

(deftest ^{:cql true} test-insert-and-select-count-using-convenience-function
  (with-thrift-exception-handling
    (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name))")
    (is (cql/void-result? (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} {:consistency "LOCAL_QUORUM" :ttl 86400})))
    (let [res (cql/execute "SELECT COUNT(*) FROM libraries")]
      (is (= 1 (count (:rows res)))))
    (cql/execute-raw "TRUNCATE libraries;")
    (cql/execute-raw "DROP COLUMNFAMILY libraries;")))


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
