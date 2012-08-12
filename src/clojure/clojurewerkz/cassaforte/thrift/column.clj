(ns clojurewerkz.cassaforte.thrift.column
  (:import [org.apache.cassandra.thrift Column]))

(defn get-name
  [^Column cdef]
  (keyword (String. (.getName cdef))))

(defn get-value
  [^Column cdef]
  (String. (.getValue cdef)))

(defn get-timestamp
  [^Column cdef]
  (.getTimestamp cdef))