(ns clojurewerkz.cassaforte.schema
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql    :as cql])
  (:use [clojurewerkz.cassaforte.conversion :only [to-keyspace-definition]])
  (:import java.util.List
           clojurewerkz.cassaforte.CassandraClient
           org.apache.cassandra.thrift.KsDef))


(defn ^org.apache.cassandra.thrift.KsDef
  describe-keyspace-raw
  [^String name]
  (.describe_keyspace ^CassandraClient cc/*cassandra-client* name))


(defn add-keyspace
  ([^KsDef keyspace-definition]
     (.system_add_keyspace ^CassandraClient cc/*cassandra-client* keyspace-definition))
  ([^String name ^String strategy-class ^List cf-defs]
     (.system_add_keyspace ^CassandraClient cc/*cassandra-client* (to-keyspace-definition name strategy-class cf-defs)))
  ([^String name ^String strategy-class ^List cf-defs & options]
     (let [args (concat [name strategy-class cf-defs] options)]
       (.system_add_keyspace ^CassandraClient cc/*cassandra-client* (apply to-keyspace-definition args)))))


(defn drop-keyspace
  [^String name]
  (.system_drop_keyspace ^CassandraClient cc/*cassandra-client* name))


(defn create-index
  "Creates an index.

   2-arity form takes a column family name and a column the index is on.
   3-arity form in addition takes an index name to use."
  ([^String column-family ^String column]
     (cql/execute "CREATE INDEX ON ? (?)" [column-family column]))
  ([^String column-family ^String column ^String index-name]
     (cql/execute "CREATE INDEX ? ON ? (?)" [index-name column-family column])))


(defn drop-index
  "Drops an index.

   1-arity form takes an index name as the only argument.
   2-arity form takes a column family name and a column the index is on."
  ([^String index-name]
     (cql/execute "DROP INDEX ?" [(cql/escape index-name)]))
  ([^String column-family ^String column]
     (cql/execute "DROP INDEX ?" [(cql/escape (str column-family "_" column "_idx"))])))
