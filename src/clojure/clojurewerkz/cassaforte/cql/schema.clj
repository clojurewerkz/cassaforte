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

   2-arity form takes a column family name and a column the index is on.
   3-arity form in addition takes an index name to use."
  ([column-family column-name]
     (create-index column-family column-name nil))
  ([column-family column-name index-name]
     (let [query (q/prepare-create-index-query column-family column-name index-name)]
       (cql/execute query))))

(defn drop-index
  "Drops an index.

   1-arity form takes an index name as the only argument.
   2-arity form takes a column family name and a column the index is on."
  ([^String index-name]
     (cql/execute "DROP INDEX ?" [index-name]))
  ([^String column-family ^String column]
     (cql/execute "DROP INDEX ?" [(str column-family "_" column "_idx")])))
