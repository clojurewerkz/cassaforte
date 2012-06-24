(ns clojurewerkz.cassaforte.client
  (import clojurewerkz.cassaforte.CassandraClient))

;;
;; API
;;

(def ^{:cost true}
  default-port 9160)

(def ^{:dynamic true :tag CassandraClient}
  *cassandra-client*)


(defn ^clojurewerkz.cassaforte.CassandraClient connect
  "Connect to a Cassandra node"
  ([^String hostname ^String keyspace]
     (connect hostname default-port keyspace))
  ([^String hostname ^long port ^String keyspace]
     (let [client (CassandraClient. hostname port keyspace)]
       client)))


(defn ^clojurewerkz.cassaforte.CassandraClient connect!
  ([^String hostname ^String keyspace]
     (connect! hostname default-port keyspace))
  ([^String hostname ^long port ^String keyspace]
     (let [client (CassandraClient. hostname port keyspace)]
       (alter-var-root (var *cassandra-client*) (constantly client))
       client)))
