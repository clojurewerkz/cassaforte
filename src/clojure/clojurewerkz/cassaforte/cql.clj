(ns clojurewerkz.cassaforte.cql
  (:require [clojurewerkz.cassaforte.cql.client :as cc]
            [clojurewerkz.cassaforte.bytes  :as cb]
            [clojurewerkz.cassaforte.cql.query-builder  :as q])
  (:use [clojure.string :only [split join]]
        [clojurewerkz.support.string :only [maybe-append interpolate-vals]]
        [clojurewerkz.support.fn :only [fpartial]]
        [clojurewerkz.cassaforte.conversion :only [from-cql-prepared-result to-map to-plain-hash]])
  (:import java.util.List
           [org.apache.cassandra.thrift Compression ConsistencyLevel CqlResult CqlRow CqlResultType]
           [org.apache.cassandra.transport Client]))

(def ^:dynamic *default-consistency-level* org.apache.cassandra.db.ConsistencyLevel/ONE)
;;
;; Implementation
;;

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

(def prepared-statements-cache (atom {}))

(defn prepare-cql-query
  "Prepares a CQL query for execution. Cassandra 1.2+ only."
  [^String query]
  (if-let [statement-id (get @prepared-statements-cache query)]
    statement-id
    (let [statement-id (.bytes (.statementId (.prepare ^Client cc/*client* query)))]
      (swap! prepared-statements-cache assoc query statement-id)
      statement-id)))

(defn execute-prepared-query
  "Executes a CQL query previously prepared using the prepare-cql-query function
   by providing the actual values for placeholders"
  [^String query ^List values]
  (to-plain-hash (:rows
                  (to-map (.toThriftResult
                           (.executePrepared ^Client cc/*client*
                                             (prepare-cql-query query)
                                             (map cb/encode values)
                                             *default-consistency-level*))))))

;;
;; API
;;

(defn ^clojure.lang.IPersistentMap
  execute-raw
  "Executes a CQL query given as a string. No argument replacement (a la JDBC) is performed."
  ([^String query]
     (execute-raw query *default-consistency-level*))
  ([^String query compression]
     (let [res (.execute cc/*client*
                         (-> query clean-up)
                         compression)]
       (to-map (.toThriftResult res)))))

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

(defn select-raw*
  [column-family & opts]
  (let [query (apply q/prepare-select-query column-family (apply concat opts))]
    (execute query)))

(defn select
  [column-family &{:keys [key-type] :or {key-type "UTF8Type"} :as opts}]
  (let [result (apply select-raw* column-family opts)]
    (to-plain-hash (:rows result) key-type)))

(defn select-prepared
  [column-family &{:keys [key-type] :or {key-type "UTF8Type"} :as opts}]
  (let [[vals query] (apply q/prepare-select-query
                            column-family (apply concat
                                                 (assoc opts
                                                   :as-prepared-statement true)))]
    (execute-prepared-query query vals)))

(defn insert
  [column-family vals & opts]
  (let [query (apply q/prepare-insert-query column-family vals opts)]
    (execute-raw query)))

(defn insert-prepared
  [column-family vals & opts]
  (let [[vals query] (apply q/prepare-insert-query
                            column-family vals
                            (apply concat
                                   (assoc opts
                                     :as-prepared-statement true)))]
    (execute-prepared-query query vals)))
