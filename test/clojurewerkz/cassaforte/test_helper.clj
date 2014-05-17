(ns clojurewerkz.cassaforte.test-helper
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))

(defn with-temporary-keyspace
  [session f]
  (try
    (drop-keyspace session :new_cql_keyspace)
    (catch Exception _ nil))

  (create-keyspace session "new_cql_keyspace"
                   (with {:replication
                          {:class "SimpleStrategy"
                           :replication_factor 1 }}))

  (use-keyspace session :new_cql_keyspace)

  (create-table session :users
                (column-definitions {:name :varchar
                                     :age  :int
                                     :city :varchar
                                     :primary-key [:name]}))

  (create-table session :user_posts
                (column-definitions {:username :varchar
                                     :post_id  :varchar
                                     :body     :text
                                     :primary-key [:username :post_id]}))

  (create-table session :user_counters
                (column-definitions {:name :varchar
                                     :user_count  :counter
                                     :primary-key [:name]}))

  (f)
  (drop-keyspace session :new_cql_keyspace))

(defmacro test-combinations
  "Run given queries in both plain and prepared modes."
  [& body]
  `(do
     ~@body
     (client/prepared ~@body)))
