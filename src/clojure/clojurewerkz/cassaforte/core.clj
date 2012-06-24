(ns clojurewerkz.cassaforte.core
  (import clojurewerkz.cassaforte.CassandraClient))

;;
;; API
;;

(def ^{:cost true}
  default-port 9160)

(def ^{:dynamic true :tag CassandraClient}
  *cassandra-client*)


(defn set-keyspace
  ([^String keyspace]
     (set-keyspace *cassandra-client* keyspace))
  ([^CassandraClient client ^String keyspace]
     (.set_keyspace client keyspace)
     client))

(defn connect
  "Connect to a Cassandra node"
  ([^String hostname ^String keyspace]
     (connect hostname default-port keyspace))
  ([^String hostname ^long port ^String keyspace]
     (let [client (CassandraClient. hostname port keyspace)]
       client)))
