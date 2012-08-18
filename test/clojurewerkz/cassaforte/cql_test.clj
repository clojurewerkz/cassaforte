(ns clojurewerkz.cassaforte.cql-test
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql])
  (:use clojure.test
        clojurewerkz.cassaforte.conversion
        clojurewerkz.cassaforte.test.helper)
  (:import java.util.UUID))

(cc/connect! "127.0.0.1" "CassaforteTest1")


;;
;; CREATE KEYSPACE, DROP KEYSPACE
;;

(deftest ^{:cql true} test-create-and-drop-keyspace-using-raw-cql
  (with-thrift-exception-handling
    (cql/execute-raw "DROP KEYSPACE \"amazeballs\";"))
  (let [query "CREATE KEYSPACE amazeballs WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1"]
    (is (cql/void-result? (cql/execute-raw query)))
    (cql/execute-raw "DROP KEYSPACE \"amazeballs\";")))


;;
;; CREATE CF, DROP CF
;;

(deftest ^{:cql true} test-create-and-drop-column-family-using-cql
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))

  (let [result (cql/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)]
    (is (cql/void-result? result))
    (is (empty? (:rows result)))
    (cql/drop-column-family "libraries")))

(deftest ^{:cql true} test-create-truncate-and-drop-column-family-using-cql
  (with-thrift-exception-handling
    (let [result (cql/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)]
      (is (cql/void-result? result))
      (is (cql/void-result? (cql/truncate "libraries")))
    (cql/drop-column-family "libraries"))))


;;
;; CREATE INDEX, DROP INDEX
;;

(deftest ^{:cql true} test-create-and-drop-an-index-using-raw-cql
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))
  (cql/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)
  (let [result (sch/create-index "libraries" :language)]
    (is (cql/void-result? result))))

;;
;; BATCH
;;

;; TBD


;;
;; INSERT with placeholders
;;

(deftest ^{:cql true} test-insert-and-select-count-using-raw-cql
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))
  (cql/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)
  (is (cql/void-result? (cql/execute-raw "INSERT INTO libraries (name, language) VALUES ('Cassaforte', 'Clojure') USING CONSISTENCY LOCAL_QUORUM AND TTL 86400")))
    (cql/truncate "libraries")
    (cql/drop-column-family "libraries"))

(deftest ^{:cql true} test-insert-and-select-count-using-prepared-cql-statement
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))
  (cql/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)

  (is (cql/void-result? (cql/execute "INSERT INTO libraries (name, language) VALUES ('?', '?') USING CONSISTENCY LOCAL_QUORUM AND TTL 86400" ["Cassaforte", "Clojure"])))
  (let [res (cql/execute "SELECT COUNT(*) FROM libraries")]
    (is (cql/rows-result? res)))

  (cql/truncate "libraries")
  (cql/drop-column-family "libraries"))


;;
;; INSERT with a map
;;

(deftest ^{:cql true} test-insert-and-select-count-using-convenience-function
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))
  (cql/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)

  (is (cql/void-result? (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} :consistency "LOCAL_QUORUM" :ttl 86400)))

  (let [res (cql/execute "SELECT COUNT(*) FROM libraries")]
    (is (= 1 (count (:rows res)))))

  (cql/truncate "libraries")
  (cql/drop-column-family "libraries"))


;;
;; UPDATE with placeholders
;;

;; TBD


;;
;; DELETE with placeholders
;;

(deftest ^{:cql true} test-delete-with-prepared-cql-statement
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))
  (cql/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)

  (is (cql/void-result? (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} :consistency "LOCAL_QUORUM" :ttl 86400)))
  (is (cql/void-result? (cql/execute "DELETE FROM libraries WHERE name = '?'" ["Cassaforte"])))
  (cql/drop-column-family "libraries"))


;;
;; DELETE with convenience function
;;

;; TBD


;;
;; Raw SELECT COUNT(*)
;;

(deftest ^{:cql true} test-select-count-with-raw-cql
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))
  (cql/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)

  (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} :consistency "LOCAL_QUORUM" :ttl 86400)
  (cql/insert "libraries" {:name "Welle" :language "Clojure"} :consistency "LOCAL_QUORUM" :ttl 86400)

  (let [res (cql/execute-raw "SELECT COUNT(*) FROM libraries")
        n   (cql/count-value res)]
    (is (= 2 n)))
  (cql/execute "DELETE FROM libraries WHERE name = '?'" ["Cassaforte"])
  (let [res (cql/execute-raw "SELECT COUNT(*) FROM libraries")
        n   (cql/count-value res)]
    (is (= 1 n)))
  (cql/drop-column-family "libraries"))


;; ;;
;; ;; Raw SELECT
;; ;;

(deftest ^{:cql true} test-select-with-raw-cql-and-utf8-named-columns
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))

  (cql/create-column-family "libraries"
                            {:name      "varchar"
                             :language  "varchar"
                             :rating    "double"
                             :votes     "int"
                             :year      "bigint"
                             :released  "boolean"}
                            :primary-key :name)

  (sch/create-index "libraries" :language)

  (cql/insert "libraries" {:name "Cassaforte" :language "Clojure" :rating 4.0 :year 2012})
  (cql/insert "libraries" {:name "Riak" :language "Erlang" :rating 5.0 :year 2009})

  (let [res (to-plain-hash (:rows (cql/execute-raw "SELECT * FROM libraries")))]
    (is (= {:name "Cassaforte" :language "Clojure" :rating 4.0 :released nil :votes nil :year 2012}
           (get res "Cassaforte")))
    (is (= {:name "Riak" :language "Erlang" :rating 5.0 :released nil :votes nil :year 2009}
           (get res "Riak"))))

  ;; By indexed column
  (let [res (to-plain-hash (:rows (cql/execute-raw "SELECT * FROM libraries WHERE language='Erlang'")))]
    (is (= {:name "Riak" :language "Erlang" :rating 5.0 :released nil :votes nil :year 2009}
           (get res "Riak"))))

  (cql/drop-column-family "libraries"))

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

(deftest ^{:cql true} test-keyspace-name-quoting
  (are [unquoted quoted] (is (= quoted (cql/quote-identifier unquoted)))
       "accounts" "\"accounts\""))
