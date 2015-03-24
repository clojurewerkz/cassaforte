(ns clojurewerkz.cassaforte.test-helper
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))

(defn make-test-session
  []
  (client/connect ["127.0.0.1"]))

(defn with-temporary-keyspace
  [session f]
  (drop-keyspace session :new_cql_keyspace (if-exists))

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

  (create-table session :events_by_device_id_and_date
                (column-definitions {:date        :varchar
                                     :device_id   :varchar
                                     :created_at  :timeuuid
                                     :payload     :text
                                     :primary-key [[:device_id :date] :created_at]}))

  (f)
  (drop-keyspace session :new_cql_keyspace))

(defmacro test-combinations
  "Run given queries in both plain and prepared modes."
  [& body]
  `(do
     ~@body
     (client/prepared ~@body)))
