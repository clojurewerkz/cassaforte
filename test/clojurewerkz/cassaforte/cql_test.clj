(ns clojurewerkz.cassaforte.cql-test
  (require [clojurewerkz.cassaforte.client :as cc]
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
    (cql/execute "DROP COLUMNFAMILY ?" ["libraries"]))
  (let [query  "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name));"
        result (cql/execute-raw query)]
    (is (cql/void-result? result))
    (is (empty? (:rows result)))
    (cql/execute "DROP COLUMNFAMILY ?" ["libraries"])))

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
    (cql/execute-raw "DROP COLUMNFAMILY libraries;"))
  (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name))")
  (let [query  "CREATE INDEX ON libraries (language)"
        result (cql/execute-raw query)]
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
    (cql/execute-raw "DROP COLUMNFAMILY libraries;"))
  (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name))")
  (is (cql/void-result? (cql/execute-raw "INSERT INTO libraries (name, language) VALUES ('Cassaforte', 'Clojure') USING CONSISTENCY LOCAL_QUORUM AND TTL 86400")))
    (cql/execute-raw "TRUNCATE libraries;")
    (cql/execute-raw "DROP COLUMNFAMILY libraries;"))


(deftest ^{:cql true} test-insert-and-select-count-using-prepared-cql-statement
  (with-thrift-exception-handling
    (cql/execute-raw "DROP COLUMNFAMILY libraries;"))
  (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name))")
    (is (cql/void-result? (cql/execute "INSERT INTO libraries (name, language) VALUES ('?', '?') USING CONSISTENCY LOCAL_QUORUM AND TTL 86400" ["Cassaforte", "Clojure"])))
    (let [res (cql/execute "SELECT COUNT(*) FROM libraries")]
      (is (cql/rows-result? res)))
    (cql/execute-raw "TRUNCATE libraries;")
    (cql/execute-raw "DROP COLUMNFAMILY libraries;"))


;;
;; INSERT with a map
;;

(deftest ^{:cql true} test-insert-and-select-count-using-convenience-function
  (with-thrift-exception-handling
    (cql/execute-raw "DROP COLUMNFAMILY libraries;"))
  (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                               language varchar,
                                               PRIMARY KEY (name))")
  (is (cql/void-result? (cql/insert "libraries" {:name "Cassaforte" :language "Clojure"} {:consistency "LOCAL_QUORUM" :ttl 86400})))
  (let [res (cql/execute "SELECT COUNT(*) FROM libraries")]
    (is (= 1 (count (:rows res)))))
  (cql/execute-raw "TRUNCATE libraries;")
  (cql/execute-raw "DROP COLUMNFAMILY libraries;"))


;;
;; UPDATE with placeholders
;;

;; TBD


;;
;; DELETE with placeholders
;;

(deftest ^{:cql true} test-delete-with-prepared-cql-statement
  (with-thrift-exception-handling
    (cql/execute-raw "DROP COLUMNFAMILY libraries;"))
  (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                                    language  varchar,
                                                    PRIMARY KEY (name))")
    (is (cql/void-result? (cql/execute "INSERT INTO libraries (name, language) VALUES ('?', '?') USING CONSISTENCY LOCAL_QUORUM AND TTL 86400" ["Cassaforte", "Clojure"])))
    (is (cql/void-result? (cql/execute "DELETE FROM libraries WHERE name = '?'" ["Cassaforte"])))
    (cql/execute-raw "DROP COLUMNFAMILY libraries;"))


;;
;; DELETE with convenience function
;;

;; TBD


;;
;; Raw SELECT COUNT(*)
;;

(deftest ^{:cql true} test-select-count-with-raw-cql
  (with-thrift-exception-handling
    (cql/execute-raw "DROP COLUMNFAMILY libraries;"))
  (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                                    language  varchar,
                                                    PRIMARY KEY (name))")
  (cql/execute "INSERT INTO libraries (name, language) VALUES ('?', '?') USING CONSISTENCY ONE" ["Cassaforte", "Clojure"])
  (cql/execute "INSERT INTO libraries (name, language) VALUES ('?', '?') USING CONSISTENCY ONE" ["Welle", "Clojure"])
  (let [res (cql/execute-raw "SELECT COUNT(*) FROM libraries")
        n   (cql/count-value res)]
    (is (= 2 n)))
  (cql/execute "DELETE FROM libraries WHERE name = '?'" ["Cassaforte"])
  (let [res (cql/execute-raw "SELECT COUNT(*) FROM libraries")
        n   (cql/count-value res)]
    (is (= 1 n)))
  (cql/execute-raw "DROP COLUMNFAMILY libraries;"))


;;
;; Raw SELECT
;;

(deftest ^{:cql true} test-select-with-raw-cql-and-utf8-named-columns
  (with-thrift-exception-handling
    (cql/execute-raw "DROP COLUMNFAMILY libraries;"))
  (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                                    language  varchar,
                                                    rating      double,
                                                    votes       int,
                                                    year        bigint,
                                                    released    boolean,
                                                    PRIMARY KEY (name))")
  (cql/execute "INSERT INTO libraries (name, language, rating, year) VALUES ('?', '?', ?, ?) USING CONSISTENCY ONE" ["Cassaforte", "Clojure", 4.0, 2012])
  (cql/execute "INSERT INTO libraries (name, language, rating, year) VALUES ('?', '?', ?, ?) USING CONSISTENCY ONE" ["Welle", "Clojure", 5.0, 2011])
  (let [res (cql/execute-raw "SELECT * FROM libraries")
        row (-> res :rows first)
        col (second (:columns row))]
    (is (= (String. ^bytes (:key row) "UTF-8") "Cassaforte"))
    (is (= (:value col) "Clojure"))
    (doseq [col (:columns row)]
      (is (:name col))
      (is (contains? col :value))
      (is (:ttl col))
      (is (:timestamp col))))
  (cql/execute-raw "TRUNCATE libraries")
  (cql/execute-raw "DROP COLUMNFAMILY libraries;"))


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
