(ns clojurewerkz.cassaforte.thrift.column
  (:import [org.apache.cassandra.thrift Column]))

(defn get-name
  [^Column c]
  (.getName c))

(defn get-value
  [^Column c]
  (.getValue c))

(defn get-timestamp
  [^Column c]
  (.getTimestamp c))

(defn get-ttl
  [^Column c]
  (.getTtl c))