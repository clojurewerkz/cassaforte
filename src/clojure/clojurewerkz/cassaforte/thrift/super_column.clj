(ns clojurewerkz.cassaforte.thrift.super-column
  (:import [org.apache.cassandra.thrift SuperColumn]))

(defn get-name
  [^SuperColumn cdef]
  (String. (.getName cdef)))

(defn get-columns
  [^SuperColumn cdef]
  (.getColumns cdef))
