(ns clojurewerkz.cassaforte.policies
  "Consistency levels, retry policies, reconnection policies, etc"
  (:require [qbits.hayt.cql :as hayt])
  (:import [com.datastax.driver.core ConsistencyLevel]
           [com.datastax.driver.core.policies
            LoadBalancingPolicy DCAwareRoundRobinPolicy RoundRobinPolicy TokenAwarePolicy
            LoggingRetryPolicy DefaultRetryPolicy DowngradingConsistencyRetryPolicy FallthroughRetryPolicy
            RetryPolicy ConstantReconnectionPolicy ExponentialReconnectionPolicy]))

;;
;; Load Balancing
;;

(defn round-robin-policy
  "Round-robin load balancing policy. Picks nodes to execute requests on in order."
  []
  (RoundRobinPolicy.))

(defn dc-aware-round-robin-policy
  "Datacenter aware load balancing policy.

   Like round-robin but over the nodes located in the same datacenter.
   Nodes from other datacenters will be tried only if all requests to local nodes fail."
  [^String local-dc]
  (DCAwareRoundRobinPolicy. local-dc))

(defn token-aware-policy
  "Takes a load balancing policy and makes it token-aware"
  [^LoadBalancingPolicy underlying-policy]
  (TokenAwarePolicy. underlying-policy))

;;
;; Retries
;;

(def retry-policies {:default                 (constantly DefaultRetryPolicy/INSTANCE)
                     :downgrading-consistency (constantly DowngradingConsistencyRetryPolicy/INSTANCE)
                     :fallthrough             (constantly FallthroughRetryPolicy/INSTANCE)})

(defn retry-policy
  [rp]
  ((rp retry-policies)))

(defn logging-retry-policy
  "A retry policy that wraps another policy, logging the decision made by its sub-policy."
  [^RetryPolicy policy]
  (LoggingRetryPolicy. policy))

(def ^:dynamic *retry-policy* (retry-policy :default))

(defmacro with-retry-policy
  "Executes a query with the given retry policy"
  [retry-policy & body]
  `(binding [*retry-policy* ~retry-policy]
     ~@body))

;;
;; Reconnection
;;

(defn exponential-reconnection-policy
  "Reconnection policy that waits exponentially longer between each
reconnection attempt but keeps a constant delay once a maximum delay is reached.

   Delays should be given in milliseconds"
  [base-delay-ms max-delay-ms]
  (ExponentialReconnectionPolicy. base-delay-ms max-delay-ms))

(defn constant-reconnection-policy
  "Reconnection policy that waits constantly longer between each
reconnection attempt but keeps a constant delay once a maximum delay is
reached.

   Delay should be given in milliseconds"
  [delay-ms]
  (ConstantReconnectionPolicy. delay-ms))

;;
;; Consistency Level
;;

(def ^:dynamic *consistency-level* :one)

(def consistency-levels
  {:any ConsistencyLevel/ANY
   :one ConsistencyLevel/ONE
   :two ConsistencyLevel/TWO
   :three ConsistencyLevel/THREE
   :quorum ConsistencyLevel/QUORUM
   :all ConsistencyLevel/ALL
   :local-quorum ConsistencyLevel/LOCAL_QUORUM
   :each-quorum ConsistencyLevel/EACH_QUORUM})

(defn consistency-level
  [cl]
  (get consistency-levels cl))

(defn resolve-consistency-level
  [cl]
  (if (= (type cl) ConsistencyLevel)
    cl
    (consistency-level cl)))

(defmacro with-consistency-level
  "Executes a query with the given consistency level"
  [consistency-level & body]
  `(binding [*consistency-level* ~consistency-level]
     ~@body))

;;
;; Prepared Statements
;;

(defmacro forcing-prepared-statements
  "Forces prepared statements for operations executed in the body"
  [& body]
  `(binding [hayt/*prepared-statement* true]
     ~@body))

(defmacro without-prepared-statements
  "Disables prepared statements for operations executed in the body"
  [& body]
  `(binding [hayt/*prepared-statement* false]
     ~@body))
