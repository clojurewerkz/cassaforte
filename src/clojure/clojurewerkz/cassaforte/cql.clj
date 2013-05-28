(ns clojurewerkz.cassaforte.cql
  (:require
   [qbits.hayt.cql :as cql]
   [clojurewerkz.cassaforte.query :as query]
   [clojurewerkz.cassaforte.client :as client]))

(defmacro prepared
  "Helper macro to execute prepared statement"
  [& body]
  `(binding [cql/*prepared-statement* true
             cql/*param-stack*        (atom [])]
     (do ~@body)))

(defn- render-query
  "Renders compiled query"
  [query-params]
  (let [renderer (if cql/*prepared-statement* query/->prepared query/->raw)]
    (renderer query-params)))

(defn- compile-query
  "Compiles query from given `builder` and `query-params`"
  [query-params builder]
  (apply builder (flatten query-params)))

(defn ^:private execute-
  [query-params builder]
  (let [rendered-query (render-query (compile-query query-params builder))]
    (client/execute rendered-query cql/*prepared-statement*)))

;;
;; Schema operations
;;

(defn drop-keyspace
  [ks]
  (execute- [ks] query/drop-keyspace-query))

(defn create-keyspace
  [& query-params]
  (execute- query-params query/create-keyspace-query))

(defn create-table
  [& query-params]
  (execute- query-params query/create-table-query))

(def create-column-family create-table)

(defn drop-table
  [ks]
  (execute- [ks] query/drop-table-query))

(defn use-keyspace
  [ks]
  (execute- [ks] query/use-keyspace-query))

(defn alter-table
  [& query-params]
  (execute- query-params query/alter-table-query))

(defn alter-keyspace
  [& query-params]
  (execute- query-params query/alter-keyspace-query))

;;
;; DB Operations
;;

(defn insert
  [& query-params]
  (execute- query-params query/insert-query))

(defn insert-batch
  [table records]
  (->> (map #(query/insert-query table %) records)
       (apply query/queries)
       query/batch-query
       render-query
       client/execute))

(defn update
  [& query-params]
  (execute- query-params query/update-query))

(defn delete
  [& query-params]
  (execute- query-params query/delete-query))

(defn select
  [& query-params]
  (execute- query-params query/select-query))

(defn truncate
  [table]
  (execute- [table] query/truncate-query))

;;
;; Higher level DB functions
;;

;; TBD, add Limit
(defn get-one
  [& query-params]
  (execute- query-params query/select-query))

(defn perform-count
  [table & query-params]
  (:count
   (first
    (select table
            (cons
             (query/columns (query/count*))
             query-params)))))

;;
;; Higher-level helper functions for schema
;;

(defn describe-keyspace
  [ks]
  (first
   (select :system.schema_keyspaces
           (query/where :keyspace_name ks))))

(defn describe-table
  [ks table]
  (first
   (select :system.schema_columnfamilies
           (query/where :keyspace_name ks
                        :columnfamily_name table))))

(defn describe-columns
  [ks table]
  (select :system.schema_columns
          (query/where :keyspace_name ks
                       :columnfamily_name table)))

;;
;; Higher-level collection manipulation
;;

(defn- get-chunk
  "Returns next chunk for the lazy world iteration"
  [table partition-key chunk-size last-pk]
  (if (nil? last-pk)
    (select table
            (query/limit chunk-size))
    (select table
            (query/where (query/token partition-key) [> (query/token last-pk)])
            (query/limit chunk-size))))

(defn iterate-world
  "Lazily iterates through the collection, returning chunks of chunk-size."
  ([table partition-key chunk-size]
     (iterate-world table partition-key chunk-size []))
  ([table partition-key chunk-size c]
     (lazy-cat c
               (let [last-pk    (get (last c) partition-key)
                     next-chunk (get-chunk table partition-key chunk-size last-pk)]
                 (iterate-world table partition-key chunk-size next-chunk)))))
