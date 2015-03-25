;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns clojurewerkz.cassaforte.client
  "Provides fundamental functions for

   * connecting to Cassandra nodes and clusters
   * configuring connections
   * tuning load balancing, retries, reconnection strategies and consistency settings
   * preparing and executing queries constructed via DSL
   * working with executing results"
  (:require [clojure.java.io                    :as io]
            [clojurewerkz.cassaforte.policies   :as cp]
            [clojurewerkz.cassaforte.conversion :as conv]
            [qbits.hayt.cql                     :as hayt])
  (:import [com.datastax.driver.core Statement ResultSet ResultSetFuture Host Session Cluster
            Cluster$Builder SimpleStatement PreparedStatement HostDistance PoolingOptions
            SSLOptions ProtocolOptions$Compression]
           [com.datastax.driver.auth DseAuthProvider]
           [com.google.common.util.concurrent Futures FutureCallback]
           java.net.URI
           [javax.net.ssl TrustManagerFactory KeyManagerFactory SSLContext]
           [java.security KeyStore SecureRandom]
           [com.datastax.driver.core.exceptions DriverException]))

(declare build-ssl-options select-compression)

(def ^:dynamic *fetch-size*)

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
     * consistency-level: default consistency level for all queires to be executed against this cluster
     * ssl: ssl options in the form {:keystore-path path :keystore-password password} Also accepts :cipher-suites with a Seq of cipher suite specs.
     * ssl-options: pre-built SSLOptions object (overrides :ssl)
     * kerberos: enables kerberos authentication"
  [{:keys [hosts
           port
           credentials
           connections-per-host
           max-connections-per-host
           consistency-level
           retry-policy
           reconnection-policy
           load-balancing-policy
           force-prepared-queries
           ssl
           ssl-options
           kerberos
           protocol-version
           compression]
    :or {protocol-version 2}}]
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
    (when protocol-version
      (.withProtocolVersion builder protocol-version))
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
    (when compression
      (.withCompression (select-compression compression)))
    (when ssl
      (.withSSL builder (build-ssl-options ssl)))
    (when ssl-options
      (.withSSL builder ssl-options))
    (when kerberos
      (.withAuthProvider builder (DseAuthProvider.)))
    (.build builder)))

(defn- ^SSLOptions build-ssl-options
  [{:keys [keystore-path keystore-password cipher-suites]}]
  (let [keystore-stream   (io/input-stream keystore-path)
        keystore          (KeyStore/getInstance "JKS")
        ssl-context       (SSLContext/getInstance "SSL")
        keymanager        (KeyManagerFactory/getInstance (KeyManagerFactory/getDefaultAlgorithm))
        trustmanager      (TrustManagerFactory/getInstance (TrustManagerFactory/getDefaultAlgorithm))
        password          (char-array keystore-password)
        ssl-cipher-suites (if cipher-suites
                            (into-array String cipher-suites)
                            SSLOptions/DEFAULT_SSL_CIPHER_SUITES)]
    (.load keystore keystore-stream password)
    (.init keymanager keystore password)
    (.init trustmanager keystore)
    (.init ssl-context (.getKeyManagers keymanager) (.getTrustManagers trustmanager) nil)
    (SSLOptions. ssl-context ssl-cipher-suites)))

(defn- ^ProtocolOptions$Compression select-compression
  [compression]
  (case compression
    :snappy ProtocolOptions$Compression/SNAPPY
    :lz4 ProtocolOptions$Compression/LZ4
    ProtocolOptions$Compression/NONE))

(defn- connect-or-close
  "Attempts to connect to the cluster or closes the cluster and reraises any errors."
  [^Cluster cluster & [keyspace]]
  (try
    (if keyspace
      (.connect cluster keyspace)
      (.connect cluster))
    (catch DriverException e
      (.close cluster)
      (throw e))))

(defn ^Session connect
  "Connects to the Cassandra cluster. Use `build-cluster` to build a cluster."
  ([hosts]
   (connect-or-close (build-cluster {:hosts hosts})))
  ([hosts keyspace-or-opts]
    (if (string? keyspace-or-opts)
      (connect hosts keyspace-or-opts {})
      (let [keyspace (:keyspace keyspace-or-opts)
            opts     (dissoc keyspace-or-opts :keyspace)]
        (if keyspace
          (connect hosts keyspace opts)
          (connect-or-close (-> opts (merge {:hosts hosts}) build-cluster))))))
  ([hosts keyspace opts]
   (let [c (build-cluster (merge opts {:hosts hosts}))]
     (connect-or-close c (name keyspace)))))

(defn ^Session connect-with-uri
  ([^String uri]
     (connect-with-uri uri {}))
  ([^String uri opts]
     (let [^URI u (URI. uri)]
       (connect [(.getHost u)] (merge {:port (.getPort u) :keyspace (-> u .getPath (.substring 1))} opts)))))

(defn disconnect
  "1-arity version receives Session, and shuts it down. It doesn't shut down all other sessions
   on same cluster."
  [^Session session]
  (.close session))

(defn disconnect!
  "Shuts the cluster and session down.  If you have other sessions, use the safe `disconnect` function instead."
  [^Session session]
  (.close (.getCluster session)))

(defn shutdown-cluster
  "Shuts down provided cluster"
  [^Cluster cluster]
  (.close cluster))

(defmacro prepared
  "Same as cassaforte.policies/forcing-prepared-statements. Kept for backwards
   compatibility."
  [& body]
  `(binding [hayt/*prepared-statement* true
             hayt/*param-stack*        (atom [])]
     (do ~@body)))

(defmacro with-fetch-size
  "Temporarily alters fetch size."
  [^Integer n# & body]
  `(binding [*fetch-size* ~n#]
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
     (.prepare ^Session session query)))

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

(defn- ^Statement statement-for
  [^Session session query prepared?]
  (if prepared?
    (if (coll? query)
      (build-statement (prepare session (first query))
                       (second query))
      (throw (IllegalArgumentException.
              "Query is meant to be executed as prepared, but no values were supplied.")))
    (build-statement query)))

(defn ^ResultSetFuture execute-async
  "Executes a pre-built query and returns a future.

   Options
     * prepared - whether the query should or should not be executed as prepared, always passed
       explicitly, because `execute` is considered to be a low-level function."
  ([^Session session query]
     (execute-async session query {}))
  ([^Session session query {:keys [prepared]}]
     (let [^Statement statement (statement-for session query prepared)
           ^ResultSetFuture fut (.executeAsync session statement)]
       (future (conv/to-clj (.getUninterruptibly fut))))))

(defn execute
  "Executes a pre-built query.

   Options
     * prepared - whether the query should or should not be executed as prepared, always passed
       explicitly, because `execute` is considered to be a low-level function."
  ([^Session session query]
     (execute session query {}))
  ([^Session session query {:keys [prepared fetch-size]}]
     (let [^Statement statement (statement-for session query prepared)
           _                    (when fetch-size
                                  (.setFetchSize statement fetch-size))
           ^ResultSetFuture fut (.executeAsync session statement)
           res                  (.getUninterruptibly fut)]
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
  [^ResultSetFuture fut {:keys [success failure]}]
  {:pre [(not (nil? success))]}
  (future (when-let [res @fut]
            (if (= (type res) Exception)
              (if (nil? failure)
                (throw res)
                (failure res))
              (success res))))
  fut)

(defn get-result
  "Get result from Future. Optional `timeout-ms` should be specified in milliseconds."
  ([^ResultSetFuture future]
     (conv/to-clj (.get future)))
  ([^ResultSetFuture future ^long timeout-ms]
     (conv/to-clj (.get future timeout-ms
                        java.util.concurrent.TimeUnit/MILLISECONDS))))
