(ns clojurewerkz.cassaforte.schema-test
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojure.test :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))


(use-fixtures :each th/initialize!)

(deftest create-drop-keyspace-test
  (drop-keyspace :new_cql_keyspace)
  (is (empty? (describe-keyspace :new_cql_keyspace)))
  (create-keyspace :new_cql_keyspace
                   (with {:replication
                          {:class              "SimpleStrategy"
                           :replication_factor 1 }})))

(deftest create-alter-keyspace-test
  (alter-keyspace :new_cql_keyspace
                  (with {:durable_writes false
                         :replication    {:class "NetworkTopologyStrategy"
                                          :dc1 1
                                          :dc2 2}}))
  (let [res (describe-keyspace :new_cql_keyspace)]
    (is (= "new_cql_keyspace" (:keyspace_name res)))
    (is (= false (:durable_writes res)))
    (is (= "{\"dc2\":\"2\",\"dc1\":\"1\"}" (:strategy_options res)))))

(deftest create-drop-table-test
  (create-table :people
                (column-definitions {:name        :varchar
                                     :title       :varchar
                                     :birth_date  :timestamp
                                     :primary-key [:name]}))

  (let [td      (describe-table "new_cql_keyspace" "people")
        columns (describe-columns "new_cql_keyspace" "people")]
    (is (= "org.apache.cassandra.db.marshal.TimestampType"
           (get-in columns [0 :validator])))
    (is (= "org.apache.cassandra.db.marshal.UTF8Type"
           (get-in columns [1 :validator])))

    (is (= 3 (count columns)))))

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

(deftest create-alter-table-add-column-test
  (create-table :userstmp
                (column-definitions {:name        :varchar
                                     :title       :varchar
                                     :primary-key [:name]}))
  (changes-by
   #(alter-table :userstmp
                 (add-column :birth_date :timestamp))
   #(count (describe-columns "new_cql_keyspace" "userstmp"))
   1)
  (drop-table :userstmp))

(deftest create-alter-table-rename
  (create-table :peopletmp
                (column-definitions {:naome       :varchar
                                     :title       :varchar
                                     :primary-key [:naome]}))

  (changes-from-to
   #(alter-table :peopletmp
                 (rename-column :naome :name))
   #(:key_aliases (describe-table "new_cql_keyspace" "peopletmp"))
   "[\"naome\"]"
   "[\"name\"]"))

(deftest create-table-compound-key
  (create-table :people
                (column-definitions {:first_name :varchar
                                     :last_name  :varchar
                                     :city       :varchar
                                     :info :text
                                     :primary-key [:first_name :last_name :city]}))
  (let [cfd (describe-table :new_cql_keyspace :people)]
    (is (= "[\"first_name\"]" (:key_aliases cfd)))
    (is (= "[\"last_name\",\"city\"]" (:column_aliases cfd)))))

(deftest create-table-composite-parition-key
  (create-table :people
                (column-definitions {:first_name :varchar
                                     :last_name  :varchar
                                     :city       :varchar
                                     :info :text
                                     :primary-key [[:first_name :last_name] :city]}))
  (let [cfd (describe-table :new_cql_keyspace :people)]
    (is (= "[\"first_name\",\"last_name\"]" (:key_aliases cfd)))
    (is (= "[\"city\"]" (:column_aliases cfd)))))
