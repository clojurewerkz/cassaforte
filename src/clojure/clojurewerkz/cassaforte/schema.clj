(ns clojurewerkz.cassaforte.schema
  (:require [clojurewerkz.cassaforte.client :as cc])
  (:use [clojurewerkz.cassaforte.conversion :only [to-keyspace-definition]])
  (:import java.util.List
           clojurewerkz.cassaforte.CassandraClient
           org.apache.cassandra.thrift.KsDef))


(defn ^org.apache.cassandra.thrift.KsDef
  describe-keyspace
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
