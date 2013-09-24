(ns clojurewerkz.cassaforte.client
  "Provides fundamental functions for

   * connecting to Cassandra nodes and clusters
   * configuring connections
   * tuning load balancing, retries, reconnection strategies and consistency settings
   * preparing and executing queries constructed via DSL
   * working with executing results"
  (:require [clojurewerkz.cassaforte.debug :as dbg]
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
;;
;; Client-related
;;

(def ^:dynamic *default-cluster*)
(def ^:dynamic *default-session*)
(def ^:dynamic *async* false)
(def ^:dynamic *debug* false)

(def ^:dynamic *consistency-level* :one)
(def ^:dynamic *retry-policy* (retry-policy :default))

(defmacro with-session
  "Executes a query with the given session"
  [session & body]
  `(binding [*default-session* ~session]
     ~@body))

(defmacro async
  "Executes a query asyncronously"
  [& body]
  `(binding [*async* true]
     ~@body))

(defmacro prepared
  "Executes a prepared statement"
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
     (dbg/catch-exceptions ~@body)))

(defn- set-statement-options-
  [^Query statement]
  (when *retry-policy*
    (.setRetryPolicy statement *retry-policy*))
  (when *consistency-level*
    (.setConsistencyLevel statement (resolve-consistency-level *consistency-level*)))
  statement)

(defn- build-statement
  "Builds a Prepare or Simple statement out of given params.

   Arities:
     * query + args - for building prepared statements, `query` is a string with placeholders, `values`
       are values to be bound to the built statement for execution.
     * query - for building simple, not prepared statements."
  ([^PreparedStatement query values]
     (set-statement-options- (.bind query (to-array values))))
  ([^String string-query]
     (set-statement-options- (SimpleStatement. string-query))))

(defn ^PreparedStatement prepare
  "Prepares the provided query on C* server for futher execution.

   This assumes that query is valid. Returns the prepared statement corresponding to the query."
  ([^String query]
     (prepare *default-session* query))
  ([^Session session ^String query]
     (.prepare ^Session session query)))

(defn build-cluster
  "Builds an instance of Cluster you can connect to.

   Options:
     * contact-points - cluster hosts to connect to
     * port - port, listening to incoming binary CQL connections (make sure you have `start_native_transport` set
       to true).
     * credentials - connection credentials in the form {:username username :password password}
     * connections-per-host - specifies core number of connections per host.
     * max-connections-per-host - maximum number of connections per host.
     * retry-policy - configures the retry policy to use for the new cluster.
     * load-balancing-policy - configures the load balancing policy to use for the new cluster.
     * force-prepared-queries - forces all queries to be executed as prepared by default
     * consistency-level - sets the default consistency level for all queires to be executed against this cluster,
       use `with-consistency-level` to specify consistency level on a per-query basis"
  [{:keys [contact-points
           port
           credentials
           connections-per-host
           max-connections-per-host

           consistency-level
           retry-policy
           reconnection-policy
           load-balancing-policy
           force-prepared-queries]}]
  (when force-prepared-queries
    (alter-var-root (var cql/*prepared-statement*) (constantly true)))

  (when consistency-level
    (alter-var-root (var *consistency-level*) (constantly (resolve-consistency-level consistency-level))))

  (let [^Cluster$Builder builder        (Cluster/builder)
        ^PoolingOptions pooling-options (.poolingOptions builder)]
    (when port
      (.withPort builder port))

    (when credentials
      (.withCredentials builder (:username credentials) (:password credentials)))

    (when connections-per-host
      (.setCoreConnectionsPerHost pooling-options HostDistance/LOCAL
                                  connections-per-host))
    (when max-connections-per-host
      (.setMaxConnectionsPerHost pooling-options HostDistance/LOCAL
                                 max-connections-per-host))
    (doseq [contact-point contact-points]
      (.addContactPoint builder contact-point))

    (when retry-policy
      (.withRetryPolicy builder retry-policy))

    (when reconnection-policy
      (.withReconnectionPolicy builder reconnection-policy))

    (when load-balancing-policy
      (.withLoadBalancingPolicy builder load-balancing-policy))
    (.build builder)))

(defn ^Session connect
  "Connects to the Cassandra cluster. Use `build` function to build cluster with all required options."
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

(defn disconnect!
  []
  (.shutdown *default-session*)
  (.shutdown *default-cluster*))

(defn render-query
  "Renders compiled query"
  [query-params]
  (let [renderer (if cql/*prepared-statement* cql/->prepared cql/->raw)]
    (renderer query-params)))

(defn compile-query
  "Compiles query from given `builder` and `query-params`"
  [query-params builder]
  (apply builder (flatten query-params)))

(defn as-prepared
  "Helper method to create prepared query from `query` string with `?` placeholders and values
   to be bound to the query."
  [query & values]
  (vector query values))

(defn execute
  "Executes built query

   Options
     * prepared - wether the query should or should not be executed as prepared, always passed
       explicitly, because `execute` is considered to be a low-level function."
  [& args]
  (let [[^Session session query & {:keys [prepared]}] (if (= (type (first args)) Session)
                                                        args
                                                        (cons *default-session* args))
        ^Query statement (if prepared
                           (if (coll? query)
                             (build-statement (prepare session (first query))
                                              (second query))
                             (throw (Exception. "Query is meant to be executed as prepared, but no values were supplied.")))
                           (build-statement query))
        ^ResultSetFuture future (.executeAsync session statement)]
    (when *debug*
      (dbg/output-debug query))
    (if *async*
      future
      (conv/to-map (.getUninterruptibly future)))))

(defn ^String export-schema
  "Exports the schema as a string"
  [^Session client]
  (-> client
      .getCluster
      .getMetadata
      .exportSchemaAsString))

(defn get-hosts
  "Returns all nodes in the cluster"
  [^Session session]
  (map (fn [^Host host]
         {:datacenter (.getDatacenter host)
          :address    (.getHostAddress (.getAddress host))
          :rack       (.getRack host)
          :is-up      (.isUp (.getMonitor host))})
       (-> session
           .getCluster
           .getMetadata
           .getAllHosts)))

;; defn get-replicas
;; defn get-cluster-name
;; defn get-keyspace
;; defn get-keyspaces
;; defn rebuild-schema

;;
;; Result Handling
;;

(defn set-callbacks
  "Set callbacks on a result future"
  [^ResultSetFuture future & {:keys [success failure]}]
  {:pre [(not (nil? success))]}
  (Futures/addCallback
   future
   (reify FutureCallback
     (onSuccess [_ result]
       (success
        (conv/to-map (.get future))))
     (onFailure [_ result]
       (failure result)))))

(defn get-result
  "Get result from Future. Optional `timeout-ms` should be specified in milliseconds."
  ([^ResultSetFuture future ^long timeout-ms]
     (conv/to-map (.get future timeout-ms
                        java.util.concurrent.TimeUnit/MILLISECONDS)))
  ([^ResultSetFuture future]
     (conv/to-map (.get future))))
