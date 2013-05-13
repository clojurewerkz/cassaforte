(ns clojurewerkz.cassaforte.client
  (:require [clojurewerkz.cassaforte.conversion :as conv]
            )
  (:import [com.datastax.driver.core Cluster Cluster$Builder Session PreparedStatement Query
            HostDistance PoolingOptions]))

(def ^{:dynamic true :tag Session}
  *client*)

(defn build-cluster
  [contact-points
   &{:keys [connections-per-host
           max-connections-per-host]}]
  (let [^Cluster$Builder builder        (Cluster/builder)
        ^PoolingOptions pooling-options (.poolingOptions builder)]
    (when connections-per-host
      (.setCoreConnectionsPerHost pooling-options HostDistance/LOCAL
                                  connections-per-host))
    (when max-connections-per-host
      (.setMaxConnectionsPerHost pooling-options HostDistance/LOCAL
                                 max-connections-per-host))
    (doseq [contact-point contact-points]
      (.addContactPoint builder contact-point))
    (.build builder)))

(defn ^Session connect
  "Connect to a Cassandra cluster"
  [hostnames]
  (.connect ^Cluster (build-cluster hostnames)))

;;
;; DB Ops
;;
(defn ^clojure.lang.IPersistentMap execute
  [^Session client query]
  (-> (.execute client query)
                conv/to-map))

(defn ^PreparedStatement prepare
  [^Session client ^String query]
  (.prepare client query))

(defn execute-prepared
  [^Session client [^String query ^java.util.List values]]
  (let [^Query prepared-statement (.bind (prepare client query) (to-array values))]
    (execute client prepared-statement)))

(defn get-hosts
  "Returns all hosts for connected cluster"
  [^Session client]
  (map conv/to-map (-> client
                       (.getCluster)
                       (.getMetadata)
                       (.getAllHosts))))

(defn export-schema
  "Exports schema as string"
  [^Session client]
  (-> client
      (.getCluster)
      (.getMetadata)
      (.exportSchemaAsString)))


;; defn get-replicas
;; defn get-cluster-name
;; defn get-keyspace
;; defn get-keyspaces
;; defn rebuild-schema

;; Add load balancing policy
;; add compression
;; DCAwareRoundRobinPolicy(String localDc) {
;; RoundRobinPolicy() {}
;; TokenAwarePolicy(LoadBalancingPolicy childPolicy) {
;; Compression/NONE / Compression/SNAPPY
;; Add retry policy
;; DowngradingConsistencyRetryPolicy/INSTANCE
;; FallthroughRetryPolicy/INSTANCE
;; LoggingRetryPolicy/INSTANCE
