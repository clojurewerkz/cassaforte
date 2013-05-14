(ns clojurewerkz.cassaforte.test-helper
  (:require [clojurewerkz.cassaforte.embedded :as e]
            [clojurewerkz.cassaforte.client :as client])
  (:use clojurewerkz.cassaforte.cql
        clojurewerkz.cassaforte.query))

(declare cluster-client)

(defn run!
  [f]
  ;; clear previous broken/interupted runs
  (try (drop-keyspace :new_cql_keyspace)
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
  (f)
  (drop-keyspace :new_cql_keyspace))

(defn initialize!
  [f]
  (e/start-server!)
  (when (not (bound? (var cluster-client)))
    ;; (def cluster-client (connect! ["192.168.60.10" "192.168.60.11" "192.168.60.12"]))
    ;; (.getAllHosts (.getMetadata (.getCluster cluster-client)))
    (def cluster (client/cluster ["127.0.0.1"]
                                 ;; :port 19042
                                 ))
    (def session (client/connect cluster)))

  (client/with-session session
    (run! f)))

(defmacro test-combinations [& body]
  `(do
     ~@body
     (prepared ~@body)))
