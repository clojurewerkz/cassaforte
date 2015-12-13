(ns clojurewerkz.cassaforte.schema-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :refer [*session* with-keyspace]]
            [clojurewerkz.cassaforte.cql         :refer :all]
            [clojure.test                        :refer :all]))

(defn changes-by
  [f f2 n]
  (let [r (f2)]
    (f)
    (is (= n (- (f2) r)))))

(defn changes-from-to
  [f f2 from to]
  (is (= from (f2)))
  (f)
  (is (= to (f2))))

(use-fixtures :each with-keyspace)

(deftest test-create-drop-table
  (create-table *session* :people
                (column-definitions {:name        :varchar
                                     :title       :varchar
                                     :birth_date  :timestamp
                                     :primary-key [:name]})
                (if-not-exists))

  (let [columns (describe-columns *session* "new_cql_keyspace" "people")]
    (is (or
         (= "org.apache.cassandra.db.marshal.DateType"
            (get-in columns [0 :validator]))
         (= "org.apache.cassandra.db.marshal.TimestampType"
            (get-in columns [0 :validator]))))
    (is (= "org.apache.cassandra.db.marshal.UTF8Type"
           (get-in columns [1 :validator])))

    (is (or
         (= 2 (count columns)) ;; Cassandra versions
         (= 3 (count columns))))))

(deftest test-create-alter-keyspace
  (alter-keyspace *session* "new_cql_keyspace"
                  (with {:durable-writes false
                         :replication    {"class" "NetworkTopologyStrategy"
                                          "dc1"   1
                                          "dc2"   2}}))
  (let [res (describe-keyspace *session* "new_cql_keyspace")]
    (is (= "new_cql_keyspace" (:keyspace_name res)))
    (is (= false (:durable_writes res)))
    (is (= "{\"dc2\":\"2\",\"dc1\":\"1\"}" (:strategy_options res)))))

(deftest test-create-table-with-indices
  (create-table *session* :people
                (column-definitions {:name        :varchar
                                     :title       :varchar
                                     :birth_date  :timestamp
                                     :primary-key [:name]}))
  (create-index *session* :people_title
                (on-table :people)
                (and-column :title))
  ;; (drop-index *session* :people_title)
  (drop-table *session* :people))

(deftest test-create-alter-table-add-column
  (create-table *session* :userstmp
                (column-definitions {:name        :varchar
                                     :title       :varchar
                                     :primary-key [:name]}))
  (changes-by
   #(alter-table *session* :userstmp
                 (add-column :birth_date :timestamp))
   #(count (describe-columns *session* "new_cql_keyspace" "userstmp"))
   1)
  (drop-table *session* :userstmp))

(deftest test-create-alter-table-rename
    (create-table *session* :peopletmp
                  (column-definitions {:naome       :varchar
                                       :title       :varchar
                                       :primary-key [:naome]}))

    (changes-from-to
     #(alter-table *session* :peopletmp
                   (rename-column :naome :name))
     #(:key_aliases (describe-table *session* "new_cql_keyspace" "peopletmp"))
     "[\"naome\"]"
     "[\"name\"]"))

(deftest test-create-table-with-compound-key
  (create-table *session* :people
                (column-definitions {:first_name :varchar
                                     :last_name  :varchar
                                     :city       :varchar
                                     :info :text
                                     :primary-key [:first_name :last_name :city]}))
  (let [cfd (describe-table *session* "new_cql_keyspace" "people")]
    (is (= "[\"first_name\"]" (:key_aliases cfd)))
    (is (= "[\"last_name\",\"city\"]" (:column_aliases cfd)))))

(deftest test-create-table-with-composite-parition-key
  (create-table *session* :people
                (column-definitions {:first_name :varchar
                                     :last_name  :varchar
                                     :city       :varchar
                                     :info :text
                                     :primary-key [[:first_name :last_name] :city]}))
  (let [cfd (describe-table *session* "new_cql_keyspace" :people)]
    (is (= "[\"first_name\",\"last_name\"]" (:key_aliases cfd)))
    (is (= "[\"city\"]" (:column_aliases cfd)))))
