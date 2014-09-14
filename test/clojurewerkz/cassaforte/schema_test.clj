(ns clojurewerkz.cassaforte.schema-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojure.test :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))

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

(let [s (client/connect ["127.0.0.1"])]
  (use-fixtures :each (fn [f]
                        (try
                          (drop-keyspace s :new_cql_keyspace)
                          (catch Exception _ nil))
                        (create-keyspace s "new_cql_keyspace"
                                         (with {:replication
                                                {:class "SimpleStrategy"
                                                 :replication_factor 1 }}))
                        (use-keyspace s "new_cql_keyspace")
                        (f)))

  (deftest test-create-drop-keyspace
    (try
      (drop-keyspace s :new_cql_keyspace)
      (catch Exception _ nil))
    (is (nil? (describe-keyspace s :new_cql_keyspace)))
    (create-keyspace s :new_cql_keyspace
                     (with {:replication
                            {:class              "SimpleStrategy"
                             :replication_factor 1 }}))
    (is (describe-keyspace s :new_cql_keyspace)))

  (deftest test-create-alter-keyspace
    (alter-keyspace s :new_cql_keyspace
                    (with {:durable_writes false
                           :replication    {:class "NetworkTopologyStrategy"
                                            :dc1 1
                                            :dc2 2}}))
    (let [res (describe-keyspace s :new_cql_keyspace)]
      (is (= "new_cql_keyspace" (:keyspace_name res)))
      (is (= false (:durable_writes res)))
      (is (= "{\"dc2\":\"2\",\"dc1\":\"1\"}" (:strategy_options res)))))

  (deftest test-create-drop-table
    (create-table s :people
                  (column-definitions {:name        :varchar
                                       :title       :varchar
                                       :birth_date  :timestamp
                                       :primary-key [:name]}))

    (let [td      (describe-table s "new_cql_keyspace" "people")
          columns (describe-columns s "new_cql_keyspace" "people")]
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

  (deftest test-create-table-with-indices
    (create-table s :people
                  (column-definitions {:name        :varchar
                                       :title       :varchar
                                       :birth_date  :timestamp
                                       :primary-key [:name]}))
    (create-index s :people :title
                  (index-name :people_title))
    (drop-index   s :people_title)
    (drop-table s :people))

  (deftest test-create-alter-table-add-column
    (create-table s :userstmp
                  (column-definitions {:name        :varchar
                                       :title       :varchar
                                       :primary-key [:name]}))
    (changes-by
     #(alter-table s :userstmp
                   (add-column :birth_date :timestamp))
     #(count (describe-columns s "new_cql_keyspace" "userstmp"))
     1)
    (drop-table s :userstmp))

  (deftest test-create-alter-table-rename
    (create-table s :peopletmp
                  (column-definitions {:naome       :varchar
                                       :title       :varchar
                                       :primary-key [:naome]}))

    (changes-from-to
     #(alter-table s :peopletmp
                   (rename-column :naome :name))
     #(:key_aliases (describe-table s "new_cql_keyspace" "peopletmp"))
     "[\"naome\"]"
     "[\"name\"]"))

  (deftest test-create-table-with-compound-key
    (create-table s :people
                  (column-definitions {:first_name :varchar
                                       :last_name  :varchar
                                       :city       :varchar
                                       :info :text
                                       :primary-key [:first_name :last_name :city]}))
    (let [cfd (describe-table s :new_cql_keyspace :people)]
      (is (= "[\"first_name\"]" (:key_aliases cfd)))
      (is (= "[\"last_name\",\"city\"]" (:column_aliases cfd)))))

  (deftest test-create-table-with-composite-parition-key
    (create-table s :people
                  (column-definitions {:first_name :varchar
                                       :last_name  :varchar
                                       :city       :varchar
                                       :info :text
                                       :primary-key [[:first_name :last_name] :city]}))
    (let [cfd (describe-table s :new_cql_keyspace :people)]
      (is (= "[\"first_name\",\"last_name\"]" (:key_aliases cfd)))
      (is (= "[\"city\"]" (:column_aliases cfd))))))
