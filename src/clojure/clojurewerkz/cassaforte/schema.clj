(ns clojurewerkz.cassaforte.schema
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql    :as cql]
            [clojurewerkz.cassaforte.query  :as q])
  (:use [clojurewerkz.cassaforte.thrift.keyspace-definition :only [build-keyspace-definition]])
  (:import java.util.List
           clojurewerkz.cassaforte.CassandraClient
           org.apache.cassandra.thrift.KsDef))


(defn ^org.apache.cassandra.thrift.KsDef
  describe-keyspace-raw
  [^String name]
  (.set_keyspace ^CassandraClient cc/*cassandra-client* name)
  (.describe_keyspace ^CassandraClient cc/*cassandra-client* name))


(defn add-keyspace
  ([^KsDef keyspace-definition]
     (.system_add_keyspace ^CassandraClient cc/*cassandra-client* keyspace-definition))
  ([^String name ^String strategy-class ^List cf-defs]
     (.system_add_keyspace ^CassandraClient cc/*cassandra-client* (build-keyspace-definition name strategy-class cf-defs)))
  ([^String name ^String strategy-class ^List cf-defs & options]
     (let [args (concat [name strategy-class cf-defs] options)]
       (.system_add_keyspace ^CassandraClient cc/*cassandra-client* (apply build-keyspace-definition args)))))

(defn set-keyspace
  [ks-name]
  (.set_keyspace ^CassandraClient cc/*cassandra-client* ks-name))

(defn describe-keyspace
  [name]
  (.describe_keyspace ^CassandraClient cc/*cassandra-client* name))

(defn update-keyspace
  [^KsDef ks-def]
  (.system_update_keyspace ^CassandraClient cc/*cassandra-client* ks-def))

(defn drop-keyspace
  [^String name]
  (.system_drop_keyspace ^CassandraClient cc/*cassandra-client* name))


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
