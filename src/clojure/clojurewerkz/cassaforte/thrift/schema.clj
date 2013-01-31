(ns clojurewerkz.cassaforte.thrift.schema
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.conversion :as conv]
            [clojurewerkz.cassaforte.ddl.column-family-definition :as cfdef])
  (:use [clojurewerkz.cassaforte.ddl.keyspace-definition :only [build-keyspace-definition]])
  (:import java.util.List
           clojurewerkz.cassaforte.CassandraClient
           org.apache.cassandra.thrift.KsDef))

(defn set-keyspace
  [ks-name]
  (.set_keyspace ^CassandraClient cc/*cassandra-client* ks-name))

(defn add-keyspace
  ([^KsDef keyspace-definition]
     (.system_add_keyspace ^CassandraClient cc/*cassandra-client* keyspace-definition))
  ([^String name ^String strategy-class ^List cf-defs & options]
     (let [args (concat [name strategy-class
                         (map #(cfdef/set-keyspace % name) cf-defs)]
                        options)]
       (add-keyspace (apply build-keyspace-definition args)))))

(defn ^org.apache.cassandra.thrift.KsDef
  describe-keyspace
  [^String name]

  (conv/to-map (.describe_keyspace ^CassandraClient cc/*cassandra-client* name)))

(defn update-keyspace
  ([^KsDef ks-def]
     (.system_update_keyspace ^CassandraClient cc/*cassandra-client* ks-def))
  ([^String name ^String strategy-class & options]
     (let [args (concat [name strategy-class []] options)]
       (update-keyspace (apply build-keyspace-definition args)))))


(defn drop-keyspace
  [^String name]
  (.system_drop_keyspace ^CassandraClient cc/*cassandra-client* name))
