(ns clojurewerkz.cassaforte.test-helper
  (:require [clojurewerkz.cassaforte.embedded :as e]
            [clojurewerkz.cassaforte.cluster.metrics :as metrics])
  (:use clojurewerkz.cassaforte.cql
        clojurewerkz.cassaforte.query))

(declare cluster-client)

(defn run!
  [f]
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
  (f)
  (drop-keyspace :new_cql_keyspace))

(defn initialize!
  [f]
  (e/start-server!)
  (when (not (bound? (var cluster-client)))
    (def cluster-client (connect! ["127.0.0.1"]))
    (metrics/csv-reporter cluster-client))
  ;; (when (not (bound? (var cluster-client)))
  ;;   (def cluster-client (connect ["192.168.60.15"])))
  (with-client cluster-client
    (run! f)))

(defmacro test-combinations [& body]
  `(do
     ~@body
     (prepared ~@body)))
