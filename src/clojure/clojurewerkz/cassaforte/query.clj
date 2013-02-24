(ns clojurewerkz.cassaforte.query
  (:require [qbits.hayt :as hayt]
            [qbits.hayt.cql :as cql]))

;;
;; Renderers
;;

(defn ->raw
  ""
  [query]
  (binding [cql/*prepared-statement* false]
    (cql/emit-query query)))

(defn ->prepared
  ""
  [query]
  [(cql/emit-query query)
   @cql/*param-stack*])


;;
;; Queries
;;

(defn select-query
  ""
  [table & clauses]
  (hayt/query ["SELECT" :columns "FROM" :table :where :order-by :limit]
              (into {:table table :columns []} clauses)))

(defn insert-query
  ""
  [table & clauses]
  (hayt/query ["INSERT INTO" :table :values :using]
              (into {:table table}  clauses)))

(defn update-query
  ""
  [table & clauses]
  (hayt/query ["UPDATE" :table :using :set-columns :where]
              (into {:table table}  clauses)))

(defn delete-query
  ""
  [table & clauses]
  (hayt/query ["DELETE" :columns "FROM" :table :using :where]
              (into {:table table :columns []} clauses)))

(defn truncate-query
  ""
  [table]
  (hayt/query ["TRUNCATE" :table]
              {:table table}))

(defn drop-keyspace-query
  ""
  [keyspace]
  (hayt/query ["DROP KEYSPACE" :keyspace]
              {:keyspace keyspace}))

(defn drop-table-query
  ""
  [table]
  (hayt/query ["DROP TABLE" :table]
              {:table table}))

(defn drop-index-query
  ""
  [index]
  (hayt/query ["DROP INDEX" :index]
              {:index index}))

(defn create-index-query
  ""
  [table index-column & clauses]
  (hayt/query ["CREATE INDEX" :index-name "ON" :table :index-column]
              (into {:table table :index-column index-column} clauses)))

(defn create-keyspace-query
  ""
  [ks & clauses]
  (hayt/query ["CREATE KEYSPACE" :keyspace :with]
              (into {:keyspace ks} clauses)))

(defn create-table-query
  [table & clauses]
  (hayt/query ["CREATE TABLE" :table :column-definitions :with]
              (into {:table table} clauses)))

(defn alter-table-query
  [table & clauses]
  (hayt/query ["ALTER TABLE" :table :alter-column :add :with]
              (into {:table table} clauses)))

(defn alter-column-family-query
  [cf & clauses]
  (hayt/query ["ALTER COLUMNFAMILY" :column-family :alter-column :add :with]
              (into {:column-family cf} clauses)))

(defn alter-keyspace-query
  ""
  [ks & clauses]
  (hayt/query ["ALTER KEYSPACE" :keyspace :with]
              (into {:keyspace ks}
                    clauses)))

(defn batch-query
  ""
  [& clauses]
  (hayt/query ["BATCH" :using :queries "APPLY BATCH"]
              (into {} clauses)))

(defn use-keyspace-query
  ""
  [keyspace]
  (hayt/query ["USE" :keyspace]
              {:keyspace keyspace}))

;;
;; Clauses
;;

(defn columns
  ""
  [& columns]
  {:columns columns})

(defn column-definitions
  ""
  [column-definitions]
  {:column-definitions column-definitions})

(defn using
  ""
  [& args]
  {:using args})

(defn limit
  ""
  [n]
  {:limit n})

(defn order-by
  ""
  [& columns]
  {:order-by columns})

(defn queries
  ""
  [& queries]
  {:queries queries})

(defn where
  ""
  [& args]
  {:where (partition 2 args)})

(defn values
  ""
  [values]
  {:values values})

(defn set-columns
  ""
  [values]
  {:set-columns values})

(defn with
  ""
  [values]
  {:with values})

(defn index-name
  ""
  [value]
  {:index-name value})

(defn alter-column
  ""
  [& args]
  {:alter-column args})

(defn add
  ""
  [& args]
  {:add args})
