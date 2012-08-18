(ns clojurewerkz.cassaforte.cql
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.bytes  :as cb])
  (:use [clojure.string :only [split join]]
        [clojurewerkz.support.string :only [maybe-append to-byte-buffer interpolate-vals interpolate-kv]]
        [clojurewerkz.support.fn :only [fpartial]]
        [clojurewerkz.cassaforte.conversion :only [from-cql-prepared-result from-cql-result]])
  (:import clojurewerkz.cassaforte.CassandraClient
           java.util.List
           [org.apache.cassandra.thrift Compression CqlResult CqlRow CqlResultType]))


;;
;; Implementation
;;

(declare escape)

(def ^{:cons true :doc "Default compression level that is used for CQL queries"}
  default-compression (Compression/NONE))

(defn quote-identifier
  "Quotes the provided identifier"
  [identifier]
  (str "\"" (name identifier) "\""))

(defn quote
  "Quotes the provided string"
  [identifier]
  (str "'" (name identifier) "'"))

(def ^{:private true}
  maybe-append-semicolon (fpartial maybe-append ";"))

(defn- clean-up
  "Cleans up the provided query string by trimming it and appending the semicolon if needed"
  [^String query]
  (-> query .trim maybe-append-semicolon))


(defn prepare-cql-query
  "Prepares a CQL query for execution. Cassandra 1.1+ only."
  ([^String query]
     (from-cql-prepared-result (.prepare_cql_query ^CassandraClient cc/*cassandra-client* (to-byte-buffer query) default-compression)))
  ([^String query ^Compression compression]
     (from-cql-prepared-result (.prepare_cql_query ^CassandraClient cc/*cassandra-client* (to-byte-buffer query) compression))))

(defn execute-prepared-query
  "Executes a CQL query previously prepared using the prepare-cql-query function
   by providing the actual values for placeholders"
  [^long prepared-statement-id ^List values]
  (.execute_prepared_cql_query ^CassandraClient cc/*cassandra-client* prepared-statement-id values))



;;
;; API
;;

(defn escape
  [^String value]
  ;; TODO
  value)

(defn ^clojure.lang.IPersistentMap
  execute-raw
  "Executes a CQL query given as a string. No argument replacement (a la JDBC) is performed."
  ([^String query]
     (from-cql-result (.executeCqlQuery ^CassandraClient cc/*cassandra-client* (-> query clean-up))))
  ([^String query ^Compression compression]
     (from-cql-result (.executeCqlQuery ^CassandraClient cc/*cassandra-client* (-> query clean-up) compression))))

(defn execute
  "Executes a CQL query given as a string. Performs positional argument (?) replacement (a la JDBC)."
  ([^String query]
     (execute-raw query))
  ([^String query args]
     (execute-raw (interpolate-vals query args))))




(defprotocol CqlStatementResult
  (^Boolean void-result? [result] "Returns true if the provided CQL query result is of type void (carries no result set)")
  (^Boolean int-result? [result] "Returns true if the provided CQL query result is of type int (carries a single value)")
  (^Boolean rows-result? [result] "Returns true if the provided CQL query result is of type rows (carries result set)")
  (^long count-value [result] "Extracts numerical value of a COUNT query"))

(extend-protocol CqlStatementResult
  CqlResult
  (void-result?
    [^CqlResult result]
    (= (.getType result) CqlResultType/VOID))
  (int-result?
    [^CqlResult result]
    (= (.getType result) CqlResultType/INT))
  (rows-result?
    [^CqlResult result]
    (= (.getType result) CqlResultType/ROWS))

  clojure.lang.IPersistentMap
  (void-result?
    [m]
    (= (:type m) CqlResultType/VOID))
  (int-result?
    [m]
    (= (:type m) CqlResultType/INT))
  (rows-result?
    [m]
    (= (:type m) CqlResultType/ROWS))
  (count-value
    [m]
    (-> m :rows first :columns first :value)))







;;
;; CQL commands (not related to schema)
;;

(defn- using-clause
  [opts]
  (if (or (empty? opts)
          (nil? opts))
    ""
    (str "USING "
         (join " AND " (map (fn [[k v]]
                              (str (.toUpperCase (name k)) " " (str v)))
                            opts)))))

(defn insert
  "Inserts (or updates) a new row into the given column family"
  ([^String column-family m {:keys [consistency timestamp ttl] :or {consistency "LOCAL_QUORUM"} :as opts}]
     (let [xs    (seq m)
           cols  (join "," (map (comp name first) xs))
           vals  (join "," (map (comp quote str second) xs))
           using (using-clause opts)]
       (execute "INSERT INTO ? (?) VALUES (?) ?"
                [column-family cols vals using])))
  ([^String keyspace ^String column-family m opts]
     (execute (format "%s.%s" (quote-identifier keyspace) (quote-identifier column-family)) m opts)))

(defn truncate
  "Truncates a column family.

   1-arity form takes a column family name as the only argument.
   2-arity form takes keyspace and column family names."  
  ([^String column-family]
     (execute "TRUNCATE ?" [column-family]))
  ([^String keyspace ^String column-family]
     (execute "TRUNCATE ?.?" [keyspace column-family])))
