(ns clojurewerkz.cassaforte.cql-test
  (:require [clojurewerkz.cassaforte.cql.client :as cc]
            [clojurewerkz.cassaforte.cql.schema :as cql-schema]
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
  (with-native-exception-handling
    (cql/execute-raw "DROP KEYSPACE \"amazeballs\";"))
  (let [query "CREATE KEYSPACE amazeballs WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"]
    (is (cql/void-result? (cql/execute-raw query)))
    (cql/execute-raw "DROP KEYSPACE \"amazeballs\";")))


;;
;; CREATE CF, DROP CF
;;

(deftest ^{:cql true} test-create-and-drop-column-family-using-cql
  (with-native-exception-handling
    (cql-schema/drop-column-family "libraries"))

  (let [result (cql-schema/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)]
    (is (cql/void-result? result))
    (is (empty? (:rows result)))
    (cql-schema/drop-column-family "libraries")))

(deftest ^{:cql true} test-create-truncate-and-drop-column-family-using-cql
  (with-native-exception-handling
    (let [result (cql-schema/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)]
      (is (cql/void-result? result))
      (is (cql/void-result? (cql-schema/truncate "libraries")))
    (cql-schema/drop-column-family "libraries"))))


;;
;; BATCH
;;

;; TBD


;;
;; INSERT with placeholders
;;

(deftest ^{:cql true} test-insert-and-select-count-using-raw-cql
  (with-native-exception-handling
    (cql-schema/drop-column-family "libraries"))
  (cql-schema/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)
  (is (cql/void-result? (cql/execute-raw "INSERT INTO libraries (name, language) VALUES ('Cassaforte', 'Clojure') USING TTL 86400")))
    (cql-schema/truncate "libraries")
    (cql-schema/drop-column-family "libraries"))

(deftest ^{:cql true} test-insert-and-select-count-using-prepared-cql-statement
  (with-native-exception-handling
    (cql-schema/drop-column-family "libraries"))
  (cql-schema/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)

  (is (cql/void-result? (cql/execute "INSERT INTO libraries (name, language) VALUES ('?', '?') USING TTL 86400" ["Cassaforte", "Clojure"])))
  (let [res (cql/execute "SELECT COUNT(*) FROM libraries")]
    (is (cql/rows-result? res)))

  (cql-schema/truncate "libraries")
  (cql-schema/drop-column-family "libraries"))

;;
;; INSERT with a map
;;

(deftest ^{:cql true} test-insert-and-select-count-using-convenience-function
  (with-native-exception-handling
    (cql-schema/drop-column-family "libraries"))
  (cql-schema/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)

  (is (cql/void-result? (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} :ttl 86400)))

  (let [res (cql/execute "SELECT COUNT(*) FROM libraries")]
    (is (= 1 (count (:rows res)))))

  (cql-schema/truncate "libraries")
  (cql-schema/drop-column-family "libraries"))


;;
;; UPDATE with placeholders
;;

;; TBD


;;
;; DELETE with placeholders
;;

(deftest ^{:cql true} test-delete-with-prepared-cql-statement
  (with-native-exception-handling
    (cql-schema/drop-column-family "libraries"))
  (cql-schema/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)

  (is (cql/void-result? (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} :ttl 86400)))
  (is (cql/void-result? (cql/execute "DELETE FROM libraries WHERE name = '?'" ["Cassaforte"])))
  (cql-schema/drop-column-family "libraries"))


;;
;; DELETE with convenience function
;;

;; TBD


;;
;; Raw SELECT COUNT(*)
;;

(deftest ^{:cql true} test-select-count-with-raw-cql
  (with-native-exception-handling
    (cql-schema/drop-column-family "libraries"))
  (cql-schema/create-column-family "libraries" {:name "varchar" :language "varchar"} :primary-key :name)

  (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} :ttl 86400)
  (cql/insert "libraries" {:name "Welle" :language "Clojure"}  :ttl 86400)

  (let [res (cql/execute-raw "SELECT COUNT(*) FROM libraries")
        n   (cql/count-value res)]
    (is (= 2 n)))
  (cql/execute "DELETE FROM libraries WHERE name = '?'" ["Cassaforte"])
  (let [res (cql/execute-raw "SELECT COUNT(*) FROM libraries")
        n   (cql/count-value res)]
    (is (= 1 n)))
  (cql-schema/drop-column-family "libraries"))


;;
;; Raw SELECT
;;

(deftest ^{:cql true} test-select-with-raw-cql-and-utf8-named-columns
  (with-native-exception-handling
    (cql-schema/drop-column-family "libraries"))

  (cql-schema/create-column-family "libraries"
                            {:name      "varchar"
                             :language  "varchar"
                             :rating    "double"
                             :votes     "int"
                             :year      "bigint"
                             :released  "boolean"}
                            :primary-key :name)

  (cql-schema/create-index "libraries" "language")

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

  (cql-schema/drop-column-family "libraries"))

;;
;; SELECT with generated query
;;

(deftest ^{:cql true} test-select-with-generated-query
  (with-native-exception-handling
    (cql-schema/drop-column-family "time_series"))

  (cql-schema/create-column-family "time_series"
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

  (cql-schema/drop-column-family "time_series"))

(deftest ^{:cql true} test-composite-keys
  (with-native-exception-handling
    (cql-schema/drop-column-family "posts"))

  (cql-schema/create-column-family "posts"
                            {:userid :text
                             :posted_at :timestamp
                             :entry_title :text
                             :content :text}
                            :primary-key [:userid :posted_at])


  (doseq [i (range 1 10)]
    (cql/insert "posts" {:userid "user1" :posted_at (str "2012-01-0" i) :entry_title (str "title" i) :content (str "content" i)})
    (cql/insert-prepared "posts" {:userid "user2" :posted_at (java.util.Date. 112 0 i 1 0 0) :entry_title (str "title" i) :content (str "content" i)}))

  (testing "Ordering by key part with exact match"
    (is (= "content1"
           (:content (first
                      (cql/select "posts" :where {:userid "user1" :posted_at [> "2011-01-05"]}
                                  :order [:posted_at :asc])))))

    (is (= "content9"
           (:content (first
                      (cql/select "posts" :where {:userid "user1" :posted_at [> "2011-01-05"]}
                                  :order [:posted_at :desc]))))))

  (testing "Range queries with open end"
    (is (= 9 (count (cql/select "posts" :where {:userid "user1" :posted_at [> "2011-01-01"]}))))
    (is (= 8 (count (cql/select "posts" :where {:userid "user1" :posted_at [> "2012-01-01"]})))))

  (testing "Range queries"
    (is (= 3 (count (cql/select "posts" :where {:userid "user1" :posted_at [> "2012-01-01" < "2012-01-05"]}))))
    (is (= 5 (count (cql/select "posts" :where {:userid "user1" :posted_at [>= "2012-01-01" <= "2012-01-05"]})))))

  (testing "Range queries and IN clause"
    (is (= 18 (count (cql/select "posts" :where {:userid [:in ["user1" "user2"]] :posted_at [> "2011-01-01"]}))))
    (is (= 16 (count (cql/select "posts" :where {:userid [:in ["user1" "user2"]] :posted_at [> "2012-01-01"]}))))
    (is (= 6 (count (cql/select "posts" :where {:userid [:in ["user1" "user2"]] :posted_at [> "2012-01-01" < "2012-01-05"]}))))
    (is (= 10 (count (cql/select "posts" :where {:userid [:in ["user1" "user2"]] :posted_at [>= "2012-01-01" <= "2012-01-05"]}))))

    (is (= 10 (count (cql/select "posts" :where {:userid [:in ["user1" "user2"]] :posted_at [> "2011-01-01"]} :limit 10)))))

  (testing "With a prepared query"
    (is (= 4 (count (cql/select-prepared "posts" :where {:userid "user1" :posted_at [> (java.util.Date. 112 0 5 1 0 0)]}))))
    (is (= 2 (count (cql/select-prepared "posts" :where {:userid "user1" :posted_at [> (java.util.Date. 112 0 5 1 0 0) < (java.util.Date. 112 0 8 1 0 0)]}))))
    (is (= 4 (count (cql/execute-prepared-query "select * from posts where userid = ? and posted_at > ? limit 10" ["user1" (java.util.Date. 112 0 5 1 0 0)]))))))

(deftest list-type-test
  (with-native-exception-handling
    (cql-schema/drop-column-family "posts"))

  (cql-schema/create-column-family "posts"
                            {:userid :varchar
                             :modified_lines "list<int>"}
                            :primary-key :userid)

  (cql/insert-prepared "posts" {:userid "user1" :modified_lines [(int 1) (int 2) (int 3)]})

  (is (= [(int 1) (int 2) (int 3)] (:modified_lines (first (cql/select "posts")))))

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
