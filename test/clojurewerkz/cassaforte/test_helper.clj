(ns clojurewerkz.cassaforte.test-helper
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))

(declare session)

(defn run!
  [f]
  ;; clear previous broken/interupted runs
  (try
    (drop-keyspace :new_cql_keyspace)
    (catch Exception _ nil))

  (create-keyspace :new_cql_keyspace
                   (with {:replication
                          {:class "SimpleStrategy"
                           :replication_factor 1 }}))

  (use-keyspace :new_cql_keyspace)

  (create-table :users
                (column-definitions {:name :varchar
                                     :age  :int
                                     :city :varchar
                                     :primary-key [:name]}))

  (create-table :user_posts
                (column-definitions {:username :varchar
                                     :post_id  :varchar
                                     :body     :text
                                     :primary-key [:username :post_id]}))

  (create-table :user_counters
                (column-definitions {:name :varchar
                                     :user_count  :counter
                                     :primary-key [:name]}))

  (f)
  (drop-keyspace :new_cql_keyspace))

(defmacro test-combinations
  "Run given queries in both plain and prepared modes."
  [& body]
  `(do
     ~@body
     (client/prepared ~@body)))

(defn initialize!
  [f]
  ;; until Cassandra Java client doesn't conflict with embedded
  ;; Cassandra 2.0. MK.
  ;; (e/start-server! :cleanup true)
  (when (not (bound? (var session)))
    (def session (client/connect! ["127.0.0.1"])))

  (client/with-session session
    (run! f)))
