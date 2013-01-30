(ns clojurewerkz.cassaforte.cql.schema
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql    :as cql]
            [clojurewerkz.cassaforte.cql.query-builder  :as q])
  (:use [clojurewerkz.cassaforte.ddl.keyspace-definition :only [build-keyspace-definition]])
  (:import java.util.List
           clojurewerkz.cassaforte.CassandraClient
           org.apache.cassandra.thrift.KsDef))


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
