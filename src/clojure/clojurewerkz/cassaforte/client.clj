(ns clojurewerkz.cassaforte.client
  (:require [flatland.useful.ns :as uns]
            [qbits.alia])
  (:import [com.datastax.driver.core Host Session]
           [com.datastax.driver.core.policies
            LoadBalancingPolicy DCAwareRoundRobinPolicy RoundRobinPolicy TokenAwarePolicy
            LoggingRetryPolicy DefaultRetryPolicy DowngradingConsistencyRetryPolicy FallthroughRetryPolicy
            RetryPolicy]))

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

;;
;; Round-robin policies
;;

(defn round-robin-policy
  "Round-robin load balancing policy. Each next query is ran on the node that was contacted
  least recently."
  []
  (RoundRobinPolicy.))

(defn dc-aware-round-robin-policy
  "Datacenter aware load balancing policy. Each next query is ran on the node that was contacted
  least recently, over the nodes located in current datacenter. Nodes from other datacenters will
  be tried only after the local nodes."
  [^String local-dc]
  (DCAwareRoundRobinPolicy. local-dc))

(defn token-aware-policy
  "Wrapper to add token-awareness to the underlying policy."
  [^LoadBalancingPolicy underlying-policy]
  (TokenAwarePolicy. underlying-policy))

(def retry-policies {:default                 DefaultRetryPolicy/INSTANCE
                     :downgrading-consistency DowngradingConsistencyRetryPolicy/INSTANCE
                     :fallthrough             FallthroughRetryPolicy/INSTANCE})

(defn logging-retry-policy
  "A retry policy that wraps another policy, logging the decision made by its sub-policy."
  [^RetryPolicy policy]
  (LoggingRetryPolicy. policy))
