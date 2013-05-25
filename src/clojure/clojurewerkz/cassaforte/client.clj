(ns clojurewerkz.cassaforte.client
  (:import [com.datastax.driver.core Host Session Cluster Cluster$Builder
            Session SimpleStatement PreparedStatement Query HostDistance PoolingOptions]
           [com.datastax.driver.core.policies
            LoadBalancingPolicy DCAwareRoundRobinPolicy RoundRobinPolicy TokenAwarePolicy
            LoggingRetryPolicy DefaultRetryPolicy DowngradingConsistencyRetryPolicy FallthroughRetryPolicy
            RetryPolicy ConstantReconnectionPolicy ExponentialReconnectionPolicy]))

(def ^:dynamic *default-cluster*)
(def ^:dynamic *default-session*)

(defmacro with-session
  "Executes query with given session"
  [session & body]
  `(binding [*default-session* ~session]
     ~@body))

(defn build-statement
  ([^PreparedStatement query args]
     (.bind query (to-array args)))
  ([^String string-query]
     (SimpleStatement. string-query)))

(defn ^PreparedStatement prepare
  [^String query]
  (.prepare ^Session *default-session* query))

(defn build-cluster
  [{:keys [contact-points
           port
           connections-per-host
           max-connections-per-host]}]
  (let [^Cluster$Builder builder        (Cluster/builder)
        ^PoolingOptions pooling-options (.poolingOptions builder)]
    (when port
      (.withPort builder port))
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
  [cluster]
  (.connect ^Cluster cluster))

(defn connect!
  "Connects and sets *default-cluster* and *default-session* for default cluster and session, that
   cql/execute is going to use."
  [hosts & {:keys [] :as options}]
  (let [cluster (build-cluster (assoc options :contact-points hosts))
        session (connect cluster)]
    (alter-var-root (var *default-cluster*) (constantly cluster))
    (alter-var-root (var *default-session*) (constantly session))
    session))

(defn export-schema
  "Exports schema as string"
  [^Session client]
  (-> client
      (.getCluster)
      (.getMetadata)
      (.exportSchemaAsString)))

(defn get-hosts
  "Returns all hosts for connected cluster"
  [^Session session]
  (map (fn [^Host host]
         {:datacenter (.getDatacenter host)
          :address    (.getHostAddress (.getAddress host))
          :rack       (.getRack host)
          :is-up      (.isUp (.getMonitor host))})
       (-> session
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

;;
;; Retry policies
;;

(def retry-policies {:default                 DefaultRetryPolicy/INSTANCE
                     :downgrading-consistency DowngradingConsistencyRetryPolicy/INSTANCE
                     :fallthrough             FallthroughRetryPolicy/INSTANCE})

(defn logging-retry-policy
  "A retry policy that wraps another policy, logging the decision made by its sub-policy."
  [^RetryPolicy policy]
  (LoggingRetryPolicy. policy))

;;
;; Reconnection policies
;;

(defn exponential-reconnection-policy
  "Reconnection policy that waits exponentially longer between each
reconnection attempt (but keeps a constant delay once a maximum delay is
reached).

   Delays should be given in milliseconds"
  [base-delay-ms max-delay-ms]
  (ExponentialReconnectionPolicy. base-delay-ms max-delay-ms))

(defn constant-reconnection-policy
  "Reconnection policy that waits constantly longer between each
reconnection attempt (but keeps a constant delay once a maximum delay is
reached).

   Delay should be given in milliseconds"
  [delay-ms]
  (ConstantReconnectionPolicy. delay-ms))
