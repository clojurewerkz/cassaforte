(ns clojurewerkz.cassaforte.cql.query-builder-test
  (:use clojure.test
        clojurewerkz.cassaforte.cql.query-builder
        clojurewerkz.cassaforte.utils))

(deftest t-prepare-create-column-family-query
  (is (= "CREATE TABLE libraries (name varchar, language varchar, rating double, PRIMARY KEY (name));"
         (prepare-create-column-family-query "libraries"
                                             {:name :varchar :language :varchar :rating :double }
                                             :primary-key :name)))
  (is (= "CREATE TABLE posts (content text, posted_at timestamp, entry_title text, userid text, PRIMARY KEY (userid, posted_at));"
         (prepare-create-column-family-query "posts"
                                             {:userid :text :posted_at :timestamp :entry_title :text :content :text}
                                             :primary-key [:userid :posted_at])))

  (is (= "CREATE TABLE libraries (name varchar, language varchar, rating double);"
         (prepare-create-column-family-query "libraries"
                                             {:name :varchar :language :varchar :rating :double }))))

(deftest t-prepare-drop-column-family-query
  (is (= "DROP TABLE libraries;"
         (prepare-drop-column-family-query "libraries"))))

(deftest t-prepare-insert-query
  (is (= "INSERT INTO libraries (name, language, rating) VALUES ('name', 'language', 1.0) USING CONSISTENCY ONE AND TTL 100;"
         (prepare-insert-query "libraries" {:name "name" :language "language" :rating 1.0}
                               :consistency "ONE"
                               :ttl 100))))


(deftest t-prepare-create-index-query
  (is (= "CREATE INDEX ON column_family_name (column_name)"
         (prepare-create-index-query "column_family_name" "column_name")))
  (is (= "CREATE INDEX index_name ON column_family_name (column_name)"
         (prepare-create-index-query "column_family_name" "column_name" "index_name"))))


;;
;; Conversion to CQL values, escaping
;;

(deftest ^{:cql true} test-conversion-to-cql-values
  (are [val cql] (is (= cql (to-cql-value val)))
    nil "null"
    10  "10"
    10N "10"
    :age "age"))


(deftest t-prepare-select-query
  (is (= "SELECT * FROM column_family_name"
         (prepare-select-query "column_family_name")))
  (is (= "SELECT column_name_1, column_name_2 FROM column_family_name"
         (prepare-select-query "column_family_name" :columns ["column_name_1" "column_name_2"])))
  (is (= "SELECT * FROM column_family_name WHERE key = 1"
         (prepare-select-query "column_family_name" :where {:key 1})))
  (is (= "SELECT * FROM column_family_name WHERE key IN (1, 2, 3)"
         (prepare-select-query "column_family_name" :where {:key [:in [1 2 3]]})))
  (is (= "SELECT * FROM column_family_name WHERE key > 1"
         (prepare-select-query "column_family_name" :where {:key [> 1]})))
  (is (= "SELECT * FROM column_family_name WHERE column_1 >= 1 AND column_1 <= 5 AND column_2 >= 1"
         (prepare-select-query "column_family_name" :where {:column_1 [>= 1 <= 5] :column_2 [>= 1]})))
  (is (= "SELECT * FROM column_family_name WHERE column_1 >= 1 AND column_2 <= 5"
         (prepare-select-query "column_family_name" :where {:column_1 [>= 1] :column_2 [<= 5]})))
  (is (= "SELECT * FROM column_family_name WHERE column_1 >= 1 AND column_2 <= 5 LIMIT 5"
         (prepare-select-query "column_family_name" :where {:column_1 [>= 1] :column_2 [<= 5]} :limit 5))))
