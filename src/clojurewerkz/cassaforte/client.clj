(ns clojurewerkz.cassaforte.client
  (:require [flatland.useful.ns :as uns]
            [qbits.alia]))

(uns/alias-ns 'qbits.alia)

(comment
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
        (.exportSchemaAsString))))


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
