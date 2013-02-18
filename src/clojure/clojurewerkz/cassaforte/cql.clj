(ns clojurewerkz.cassaforte.cql
  (:require [clojurewerkz.cassaforte.cql.client :as cc]
            [clojurewerkz.cassaforte.bytes  :as cb]
            [clojurewerkz.cassaforte.conversion :as conv]
            [clojurewerkz.cassaforte.cql.query :as query]
            [clojurewerkz.cassaforte.utils :as utils]
            [qbits.hayt.cql :as cql])
  (:import [org.apache.cassandra.transport Client]))

(def ^:dynamic *debug-output* false)
(def ^:dynamic *default-consistency-level* org.apache.cassandra.db.ConsistencyLevel/ONE)

(def prepared-statements-cache (atom {}))

(defn prepare-cql-query
  "Prepares a CQL query for execution."
  [^String query]
  (if-let [statement-id (get @prepared-statements-cache query)]
    statement-id
    (let [statement-id (.bytes (.statementId (.prepare ^Client cc/*client* query)))]
      (swap! prepared-statements-cache assoc query statement-id)
      statement-id)))

(defn execute-prepared
  "Executes a CQL query previously prepared using the prepare-cql-query function
   by providing the actual values for placeholders"
  ([q]
     (execute-prepared q *default-consistency-level*))
  ([[^String query ^java.util.List values] consistency-level]
     (conv/to-plain-hash (:rows
                          (conv/to-map (.toThriftResult
                                        (.executePrepared ^Client cc/*client*
                                                          (prepare-cql-query query)
                                                          (map cb/encode values)
                                                          consistency-level)))))))


(defn ^clojure.lang.IPersistentMap execute-raw
  ([query]
     (execute-raw query *default-consistency-level*))
  ([^String query consistency-level]
     (-> (.execute cc/*client* query consistency-level)
         (.toThriftResult)
         conv/to-map
         (:rows)
         conv/to-plain-hash)))

;; Execute could be a protocol, taht takes either string or map, converts map to string (renders query when
;; needed?

;; Ability to turn on and off prepared statements by default? Turn on prepared statements on per-query basis
;; (macro+binding)

;; Result of query rendering should be pushed stragiht to execute, it should figure out wether to
;; run the prepared or regular query itself, all the time

;; Add switch (as clj-http, throw exceptions)

;; Maybe add with-template kind of a helper?......

(defn maybe-output-debug
  [q]
  (when *debug-output*
    (println "Built query: " q))
  q)

(defmacro prepared
  [& body]
  `(binding [cql/*prepared-statement* true
             cql/*param-stack* (atom [])]
     (do ~@body)))

(defn execute*
  [query-params builder]
  (utils/with-native-exception-handling ;; add check for throw exceptions
    (let [renderer (if cql/*prepared-statement* query/->prepared query/->raw)
          executor (if cql/*prepared-statement* execute-prepared execute-raw)]
      (-> (apply builder (flatten query-params))
          renderer
          maybe-output-debug
          executor))))

(defn drop-keyspace
  [ks]
  (execute* [ks] query/drop-keyspace-query))

(defn create-keyspace
  [& query-params]
  (execute* query-params query/create-keyspace-query))

(defn create-table
  [& query-params]
  (execute* query-params query/create-table-query))

(defn drop-table
  [ks]
  (execute* [ks] query/drop-table-query))

(defn use-keyspace
  [ks]
  (execute* [ks] query/use-keyspace-query))

(defn insert
  [& query-params]
  (execute* query-params query/insert-query))

(defn select
  [& query-params]
  (execute* query-params query/select-query))

(defn describe-keyspace
  [ks]
  (first (select :system.schema_keyspaces
                 {:where [[:keyspace_name ks]]})))

(defn describe-table
  [ks table]
  (first (select :system.schema_columnfamilies
                 {:where [[:keyspace_name ks]
                          [:columnfamily_name table]]})))
