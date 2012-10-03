(ns clojurewerkz.cassaforte.client
  (:import [clojurewerkz.cassaforte CassandraClient]
   ))

;;
;; API
;;

(def ^{:cost true}
  default-port 9160)

(def ^{:dynamic true :tag CassandraClient}
  *cassandra-client*)

(defmacro with-client
  [client & body]
  `(binding [*cassandra-client* ~client]
     (do ~@body)))

(defn ^CassandraClient connect
  "Connect to a Cassandra node"
  ([^String hostname]
     (connect hostname default-port))
  ([^String hostname ^long port]
     (let [client (CassandraClient. hostname port)]
       client)))


(defn ^CassandraClient connect!
  ([^String hostname]
     (connect! hostname default-port))
  ([^String hostname ^long port]
     (let [client (CassandraClient. hostname port)]
       (alter-var-root (var *cassandra-client*) (constantly client))
       client)))