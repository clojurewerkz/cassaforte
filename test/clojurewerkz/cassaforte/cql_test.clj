(ns clojurewerkz.cassaforte.cql-test
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql])
  (:use clojure.test
        clojurewerkz.cassaforte.test-helper
        clojurewerkz.cassaforte.conversion
        clojurewerkz.cassaforte.utils)
  (:import java.util.UUID))

(use-fixtures :each initialize-cql)

;;
;; CREATE KEYSPACE, DROP KEYSPACE
;;

(deftest ^{:cql true} test-create-and-drop-keyspace-using-raw-cql
  (with-thrift-exception-handling
    (cql/execute-raw "DROP KEYSPACE \"amazeballs\";"))
  (let [query "CREATE KEYSPACE amazeballs WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"]
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
  (is (cql/void-result? (cql/execute-raw "INSERT INTO libraries (name, language) VALUES ('Cassaforte', 'Clojure') USING TTL 86400")))
    (cql/truncate "libraries")
    (cql/drop-column-family "libraries"))

(deftest ^{:cql true} test-insert-and-select-count-using-prepared-cql-statement
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))
  (cql/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)

  (is (cql/void-result? (cql/execute "INSERT INTO libraries (name, language) VALUES ('?', '?') USING TTL 86400" ["Cassaforte", "Clojure"])))
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

  (is (cql/void-result? (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} :ttl 86400)))

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

  (is (cql/void-result? (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} :ttl 86400)))
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

  (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} :ttl 86400)
  (cql/insert "libraries" {:name "Welle" :language "Clojure"}  :ttl 86400)

  (let [res (cql/execute-raw "SELECT COUNT(*) FROM libraries")
        n   (cql/count-value res)]
    (is (= 2 n)))
  (cql/execute "DELETE FROM libraries WHERE name = '?'" ["Cassaforte"])
  (let [res (cql/execute-raw "SELECT COUNT(*) FROM libraries")
        n   (cql/count-value res)]
    (is (= 1 n)))
  (cql/drop-column-family "libraries"))


;;
;; Raw SELECT
;;

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
    (is (= {:name "Riak" :language "Erlang" :rating 5.0 :released nil :votes nil :year 2009}
           (first res)))
    (is (= {:name "Cassaforte" :language "Clojure" :rating 4.0 :released nil :votes nil :year 2012}
           (second res))))

  (let [res (to-plain-hash (:rows (cql/execute-raw "SELECT * FROM libraries WHERE language='Erlang'")))]
    (is (= {:name "Riak" :language "Erlang" :rating 5.0 :released nil :votes nil :year 2009}
           (first res))))

  (cql/drop-column-family "libraries"))

;;
;; SELECT with generated query
;;

(deftest ^{:cql true} test-select-with-generated-query
  (with-thrift-exception-handling
    (cql/drop-column-family "time_series"))

  (cql/create-column-family "time_series"
                            {:tstamp "timestamp"
                             :description "varchar"}
                            :primary-key :tstamp)

  (cql/insert "time_series" {:tstamp "2011-02-03" :description "Description 1"})
  (cql/insert "time_series" {:tstamp "2011-02-04" :description "Description 2"})
  (cql/insert "time_series" {:tstamp "2011-02-05" :description "Description 3"})
  (cql/insert "time_series" {:tstamp "2011-02-06" :description "Description 4"})
  (cql/insert "time_series" {:tstamp "2011-02-07" :description "Description 5"})
  (cql/insert "time_series" {:tstamp "2011-02-08" :description "Description 6"})

  (is (= 1 (count (cql/select "time_series" :where { :tstamp "2011-02-03" } :key-type "DateType"))))

  (cql/drop-column-family "time_series"))

(deftest ^{:cql true} test-composite-keys
  (with-thrift-exception-handling
    (cql/drop-column-family "posts"))

  (cql/create-column-family "posts"
                            {:userid :text
                             :posted_at :timestamp
                             :entry_title :text
                             :content :text}
                            :primary-key [:userid :posted_at])

  (doseq [i (range 1 10)]
    (cql/insert "posts" {:userid "user1" :posted_at (str "2012-01-0" i) :entry_title (str "title" i) :content (str "content" i)}))

  (is (= 9 (count (cql/select "posts" :where {:userid "user1" :posted_at [> "2011-01-01"]}))))
  (is (= 8 (count (cql/select "posts" :where {:userid "user1" :posted_at [> "2012-01-01"]}))))
  (is (= 3 (count (cql/select "posts" :where {:userid "user1" :posted_at [> "2012-01-01" < "2012-01-05"]}))))
  (is (= 5 (count (cql/select "posts" :where {:userid "user1" :posted_at [>= "2012-01-01" <= "2012-01-05"]}))))
)
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
