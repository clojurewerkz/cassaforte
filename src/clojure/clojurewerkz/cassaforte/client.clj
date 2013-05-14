(ns clojurewerkz.cassaforte.client
  (:require [flatland.useful.ns :as uns]
            [qbits.alia])
  (:import [com.datastax.driver.core Host Session]))

(uns/alias-ns 'qbits.alia)

(defn export-schema
  "Exports schema as string"
  [^Session client]
  (-> client
      (.getCluster)
      (.getMetadata)
      (.exportSchemaAsString)))

(defn get-hosts
  "Returns all hosts for connected cluster"
  [^Session client]
  (map (fn [^Host host]
         {:datacenter (.getDatacenter host)
          :address    (.getHostAddress (.getAddress host))
          :rack       (.getRack host)})
       (-> client
           (.getCluster)
           (.getMetadata)
           (.getAllHosts))))

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
