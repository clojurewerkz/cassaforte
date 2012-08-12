(ns clojurewerkz.cassaforte.thrift.keyspace
  (:require [clojurewerkz.cassaforte.client :as cc])
  (:import [org.apache.cassandra.thrift KsDef]
           [java.nio ByteBuffer]
           clojurewerkz.cassaforte.CassandraClient
           java.util.List
           java.nio.ByteBuffer))

;;
;; Commands
;;

(defn add-keyspace
  [^KsDef ks-def]
  (.system_add_keyspace ^CassandraClient cc/*cassandra-client* ks-def))

(defn set-keyspace
  [ks-name]
  (.set_keyspace ^CassandraClient cc/*cassandra-client* ks-name))

(defn drop-keyspace
  [name]
  (.system_drop_keyspace ^CassandraClient cc/*cassandra-client* name))

(defn update-keyspace
  [^KsDef ks-def]
  (.system_update_keyspace ^CassandraClient cc/*cassandra-client* ks-def))

(defn describe-keyspace
  [name]
  (.describe_keyspace ^CassandraClient cc/*cassandra-client* name))

(defn describe-keyspaces
  [name]
  (.describe_keyspaces ^CassandraClient cc/*cassandra-client* name))

(defn count-keyspaces
  []
  (count (.describe_keyspaces ^CassandraClient cc/*cassandra-client*)))