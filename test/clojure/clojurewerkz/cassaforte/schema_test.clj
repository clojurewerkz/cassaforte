(ns clojurewerkz.cassaforte.schema-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :refer [*session* with-keyspace]]
            [clojurewerkz.cassaforte.cql         :refer :all]
            [clojurewerkz.cassaforte.metadata    :as    md]
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

  (let [columns (md/columns *session* "new_cql_keyspace" "people")]
    (is (= "text"  (get-in columns [0 :type :name])))
    (is (= "timestamp" (get-in columns [1 :type :name])))
    (is (= "text" (get-in columns [2 :type :name])))
    (is (= 3 (count columns)))))

(deftest test-create-alter-keyspace
  (alter-keyspace *session* "new_cql_keyspace"
                  (with {:durable-writes false
                         :replication    (array-map "class" "NetworkTopologyStrategy"
                                                    "dc1"   1)}))
  (let [res (md/keyspace *session* "new_cql_keyspace")]
    (is (= :new_cql_keyspace (:name res)))
    (is (= false (:durable-writes res)))
    ))

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
   #(count (md/columns *session* "new_cql_keyspace" "userstmp"))
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
     #(mapv :name (:primary-key (md/table *session* "new_cql_keyspace" "peopletmp")))
     [:naome]
     [:name]))

(deftest test-create-table-with-compound-key
  (create-table *session* :people
                (column-definitions {:first_name :varchar
                                     :last_name  :varchar
                                     :city       :varchar
                                     :info :text
                                     :primary-key [:first_name :last_name :city]}))
  (let [cfd (md/table *session* "new_cql_keyspace" "people")]
    (is (= [:first_name] (mapv :name (:partition-key cfd))))
    (is (= [:last_name :city] (mapv :name (:clustering-columns cfd))))))

(deftest test-create-table-with-composite-parition-key
  (create-table *session* :people
                (column-definitions {:first_name :varchar
                                     :last_name  :varchar
                                     :city       :varchar
                                     :info :text
                                     :primary-key [[:first_name :last_name] :city]}))
  (let [cfd (md/table *session* "new_cql_keyspace" :people)]
    (is (= [:first_name :last_name] (mapv :name (:partition-key cfd))))
    (is (= [:city] (mapv :name (:clustering-columns cfd))))))

