(ns clojurewerkz.cassaforte.cql.schema
  (:require [clojurewerkz.cassaforte.cql.client :as cc]
            [clojurewerkz.cassaforte.cql    :as cql]
            [clojurewerkz.cassaforte.cql.query-builder  :as q])
  (:use [clojurewerkz.cassaforte.ddl.keyspace-definition :only [build-keyspace-definition]])
  (:import java.util.List
           org.apache.cassandra.thrift.KsDef))

(defn create-column-family
  [name fields & {:keys [primary-key]}]
  (let [query (q/prepare-create-column-family-query name fields :primary-key primary-key)]
    (cql/execute-raw query)))

(defn drop-column-family
  [name]
  (let [query (q/prepare-drop-column-family-query name)]
    (cql/execute-raw query)))

(defn truncate
  "Truncates a column family.

   1-arity form takes a column family name as the only argument.
   2-arity form takes keyspace and column family names."
  ([^String column-family]
     (cql/execute "TRUNCATE ?" [column-family]))
  ([^String keyspace ^String column-family]
     (cql/execute "TRUNCATE ?.?" [keyspace column-family])))

(defn set-keyspace
  [keyspace]
  (cql/execute "USE \"?\" " [keyspace]))

(defn describe
  ([keyspace]
     (cql/select "system.schema_keyspaces" :where {:keyspace_name keyspace}))
  ([keyspace column-family & {:keys [with-columns]}]
     (let [res (first (cql/select "system.schema_columnfamilies" :where {:keyspace_name keyspace :columnfamily_name column-family}))]
       (if with-columns
         (assoc res
           :columns
           (cql/select "system.schema_columns" :where {:keyspace_name keyspace :columnfamily_name column-family}))
         res))))

(defn create-index
  "Creates an index.

   Takes a column family name and a column the index is on."
  [column-family column-name]
  (let [query (q/prepare-create-index-query column-family column-name)]
    (cql/execute query)))

(defn drop-index
  "Drops an index.

   1-arity form takes an index name as the only argument.
   2-arity form takes a column family name and a column the index is on."
  ([^String index-name]
     (cql/execute "DROP INDEX ?" [index-name]))
  ([^String column-family ^String column]
     (drop-index (str column-family "_" column "_idx"))))
