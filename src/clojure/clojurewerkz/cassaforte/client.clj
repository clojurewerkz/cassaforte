(ns clojurewerkz.cassaforte.client
  "Base namespace for connecting to Cassandra clusters, configuring your cluster connection, tuning
   things like Load Balancing, Retries, consistency and reconnection, rendering queries generated
   using the DSL, preparing them, working with asyncronous results."
  (:require [clojurewerkz.cassaforte.debug-utils :as debug-utils]
            [clojurewerkz.cassaforte.conversion :as conv]
            [qbits.hayt.cql :as cql]
            [clojurewerkz.cassaforte.query :as query])
  (:import [com.datastax.driver.core Query ResultSet ResultSetFuture Host Session Cluster
            Cluster$Builder SimpleStatement PreparedStatement Query HostDistance PoolingOptions
            ConsistencyLevel]
           [com.google.common.util.concurrent Futures FutureCallback]
           [com.datastax.driver.core.policies
            LoadBalancingPolicy DCAwareRoundRobinPolicy RoundRobinPolicy TokenAwarePolicy
            LoggingRetryPolicy DefaultRetryPolicy DowngradingConsistencyRetryPolicy FallthroughRetryPolicy
            RetryPolicy ConstantReconnectionPolicy ExponentialReconnectionPolicy]))

;;
;; Load Balancing policies
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

;;
;; Consistency Level
;;

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
  [kl]
  (kl consistency-levels))

;;
;; Client-related
;;

(def ^:dynamic *default-cluster*)
(def ^:dynamic *default-session*)
(def ^:dynamic *async* false)
(def ^:dynamic *debug* false)

(def ^:dynamic *consistency-level* (consistency-level :one))
(def ^:dynamic *retry-policy* (retry-policy :default))

(defmacro with-session
  "Executes query with given session"
  [session & body]
  `(binding [*default-session* ~session]
     ~@body))

(defmacro async
  "Executes query asyncronously"
  [& body]
  `(binding [*async* true]
     ~@body))

(defmacro prepared
  "Helper macro to execute prepared statement"
  [& body]
  `(binding [cql/*prepared-statement* true
             cql/*param-stack*        (atom [])]
     (do ~@body)))

(defmacro with-consistency-level
  "Executes a query with the given consistency level"
  [consistency-level & body]
  `(binding [*consistency-level* ~consistency-level]
     ~@body))

(defmacro with-retry-policy
  "Executes a query with the given retry policy"
  [retry-policy & body]
  `(binding [*retry-policy* ~retry-policy]
     ~@body))

(defmacro with-debug
  "Executes a query with *debug* bound to true"
  [& body]
  `(binding [*debug* true]
     (debug-utils/catch-exceptions ~@body)))

(defn- set-statement-options-
  [^Query statement]
  (when *retry-policy*
    (.setRetryPolicy statement *retry-policy*))
  (when *consistency-level*
    (.setConsistencyLevel statement *consistency-level*))
  statement)

(defn- build-statement
  ([^PreparedStatement query args]
     (set-statement-options- (.bind query (to-array args))))
  ([^String string-query]
     (set-statement-options- (SimpleStatement. string-query))))

(defn ^PreparedStatement prepare
  "Prepares the provided query on C* server for futher execution.

   This assumes that query is valid. Returns the prepared statement corresponding to the query."
  [^String query]
  (.prepare ^Session *default-session* query))

(defn build-cluster
  [{:keys [contact-points
           port
           connections-per-host
           max-connections-per-host

           consistency-level
           retry-policy]}]
  (when consistency-level
    (alter-var-root (var *consistency-level*) (constantly consistency-level)))

  (when retry-policy
    (alter-var-root (var *retry-policy*) (constantly retry-policy)))

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
  "Connects to a Cassandra cluster"
  ([^Cluster cluster]
     (.connect cluster))
  ([^Cluster cluster keyspace]
     (.connect cluster (name keyspace))))

(defn connect!
  "Connects and sets *default-cluster* and *default-session* for default cluster and session, that
   cql/execute is going to use."
  [hosts & {:keys [keyspace] :as options}]
  (let [cluster (build-cluster (assoc options :contact-points hosts))
        session (if keyspace
                  (connect cluster keyspace)
                  (connect cluster))]
    (alter-var-root (var *default-cluster*) (constantly cluster))
    (alter-var-root (var *default-session*) (constantly session))
    session))

(defn render
  "Renders compiled query"
  [query-params]
  (let [renderer (if cql/*prepared-statement* cql/->prepared cql/->raw)]
    (renderer query-params)))

(defn compile
  "Compiles query from given `builder` and `query-params`"
  [query-params builder]
  (apply builder (flatten query-params)))

(defn execute
  "Executes built query"
  [& args]
  (let [[^Session session query & {:keys [prepared]}] (if (= (type (first args)) Session)
                                                      args
                                                      (cons *default-session* args))
        ^Query statement (if prepared
                           (build-statement (prepare (first query))
                                            (second query))
                           (build-statement query))
        ^ResultSetFuture future (.executeAsync session statement)]
    (when *debug*
      (debug-utils/output-debug query))
    (if *async*
      future
      (conv/to-map (.getUninterruptibly future)))))

(defn ^String export-schema
  "Exports the schema as a string"
  [^Session client]
  (-> client
      (.getCluster)
      (.getMetadata)
      (.exportSchemaAsString)))

(defn get-hosts
  "Returns all nodes in the cluster"
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
;;
;;

(defn set-callbacks
  "Set callbacks for the future"
  [^ResultSetFuture future & {:keys [success failure]}]
  {:pre [(not (nil? success))]}
  (Futures/addCallback
   future
   (reify FutureCallback
     (onSuccess [_ result]
       (success
        (conv/to-map (deref future))))
     (onFailure [_ result]
       (failure result)))))

(defn get-result
  "Get result from Future"
  ([^ResultSetFuture future ^long timeout-ms]
     (conv/to-map (.get future timeout-ms
                        java.util.concurrent.TimeUnit/MILLISECONDS)))
  ([^ResultSetFuture future]
     (conv/to-map (deref future))))
