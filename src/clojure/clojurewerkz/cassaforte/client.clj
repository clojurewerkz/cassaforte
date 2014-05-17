;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns clojurewerkz.cassaforte.client
  "Provides fundamental functions for

   * connecting to Cassandra nodes and clusters
   * configuring connections
   * tuning load balancing, retries, reconnection strategies and consistency settings
   * preparing and executing queries constructed via DSL
   * working with executing results"
  (:require [clojurewerkz.cassaforte.policies :as cp]
            [clojurewerkz.cassaforte.conversion :as conv]
            [qbits.hayt.cql :as hayt])
  (:import [com.datastax.driver.core Statement ResultSet ResultSetFuture Host Session Cluster
            Cluster$Builder SimpleStatement PreparedStatement HostDistance PoolingOptions]
           [com.google.common.util.concurrent Futures FutureCallback]))

(def prepared-statement-cache (atom {}))

(defn flush-prepared-statement-cache!
  []
  (reset! prepared-statement-cache {}))

(defprotocol DummySession
  (executeAsync [_ query]))

(deftype DummySessionImpl []
  DummySession
  (executeAsync [_ query] (throw (Exception. "Not connected"))))

(defn ^Cluster build-cluster
  "Builds an instance of Cluster you can connect to.

   Options:
     * hosts: hosts to connect to
     * port: port, listening to incoming binary CQL connections (make sure you have `start_native_transport` set to true).
     * credentials: connection credentials in the form {:username username :password password}
     * connections-per-host: specifies core number of connections per host.
     * max-connections-per-host: maximum number of connections per host.
     * retry-policy: configures the retry policy to use for the new cluster.
     * load-balancing-policy: configures the load balancing policy to use for the new cluster.
     * force-prepared-queries: forces all queries to be executed as prepared by default
     * consistency-level: default consistency level for all queires to be executed against this cluster"
  [{:keys [hosts
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
    (alter-var-root (var hayt/*prepared-statement*)
                    (constantly true)))
  (when consistency-level
    (alter-var-root (var cp/*consistency-level*)
                    (constantly (cp/resolve-consistency-level consistency-level))))
  (let [^Cluster$Builder builder        (Cluster/builder)
        ^PoolingOptions pooling-options (PoolingOptions.)]
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
    (.withPoolingOptions builder pooling-options)
    (doseq [h hosts]
      (.addContactPoint builder h))
    (when retry-policy
      (.withRetryPolicy builder retry-policy))
    (when reconnection-policy
      (.withReconnectionPolicy builder reconnection-policy))
    (when load-balancing-policy
      (.withLoadBalancingPolicy builder load-balancing-policy))
    (.build builder)))

(defn ^Session connect
  "Connects to the Cassandra cluster. Use `build` function to build cluster with all required options."
  ([hosts]
     (flush-prepared-statement-cache!)
     (.connect (build-cluster {:hosts hosts})))
  ([hosts keyspace]
     (flush-prepared-statement-cache!)
     (let [c (build-cluster {:hosts hosts})]
       (.connect c (name keyspace))))
  ([hosts keyspace opts]
     (flush-prepared-statement-cache!)
     (let [c (build-cluster (merge opts {:hosts hosts}))]
       (.connect c (name keyspace)))))

(defn disconnect
  "1-arity version receives Session, and shuts it down. It doesn't shut down all other sessions
   on same cluster."
  [^Session session]
  (.shutdown session))

(defn shutdown-cluster
  "Shuts down provided cluster"
  [^Cluster cluster]
  (.shutdown cluster))

;;
;; Query, Prepared statements
;;

(defmacro prepared
  "Executes a prepared statement"
  [& body]
  `(binding [hayt/*prepared-statement* true
             hayt/*param-stack*        (atom [])]
     (do ~@body)))

(defn- set-statement-options-
  [^Statement statement]
  (when cp/*retry-policy*
    (.setRetryPolicy statement cp/*retry-policy*))
  (when cp/*consistency-level*
    (.setConsistencyLevel statement (cp/resolve-consistency-level cp/*consistency-level*)))
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
  ([^Session session ^String query]
     (if-let [cached (get @prepared-statement-cache [session query])]
       cached
       (let [prepared (.prepare ^Session session query)]
         (swap! prepared-statement-cache #(assoc % [session query] prepared))
         prepared))))

(defn render-query
  "Renders compiled query"
  [query-params]
  (let [renderer (if hayt/*prepared-statement*
                   hayt/->prepared
                   hayt/->raw)]
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

(defn ^ResultSetFuture execute-async
  "Executes a pre-built query and returns a future.

   Options
     * prepared - whether the query should or should not be executed as prepared, always passed
       explicitly, because `execute` is considered to be a low-level function."
  ([^Session session query]
     (execute-async session query {}))
  ([^Session session query {:keys [prepared]}]
     (let [^Statement statement (if prepared
                                  (if (coll? query)
                                    (build-statement (prepare session (first query))
                                                     (second query))
                                    (throw (IllegalArgumentException.
                                            "Query is meant to be executed as prepared, but no values were supplied.")))
                                  (build-statement query))]
       (.executeAsync session statement))))

(defn execute
  "Executes a pre-built query.

   Options
     * prepared - whether the query should or should not be executed as prepared, always passed
       explicitly, because `execute` is considered to be a low-level function."
  ([^Session session query]
     (execute session query {}))
  ([^Session session query opts]
     (let [^ResultSetFuture future (execute-async session query opts)
           res                     (.getUninterruptibly future)]
       (conv/to-clj res))))

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
          :is-up      (.isUp host)})
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
  [^ResultSetFuture future {:keys [success failure]}]
  {:pre [(not (nil? success))]}
  (Futures/addCallback
   future
   (reify FutureCallback
     (onSuccess [_ result]
       (success
        (conv/to-clj (.get future))))
     (onFailure [_ result]
       (failure result)))))

(defn get-result
  "Get result from Future. Optional `timeout-ms` should be specified in milliseconds."
  ([^ResultSetFuture future]
     (conv/to-clj (.get future)))
  ([^ResultSetFuture future ^long timeout-ms]
     (conv/to-clj (.get future timeout-ms
                        java.util.concurrent.TimeUnit/MILLISECONDS))))
