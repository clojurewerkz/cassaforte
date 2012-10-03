(ns clojurewerkz.cassaforte.thrift.column
  (:use    [clojurewerkz.support.string :only [to-byte-buffer]]
           [clojurewerkz.cassaforte.bytes :only [encode]])
  (:import [org.apache.cassandra.thrift Column]))

;;
;; Getters
;;

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

;;
;; Builders
;;

(defn ^Column build-column
  "Converts clojure map to column"
  ([^String key ^String value]
     (build-column to-byte-buffer key value (System/currentTimeMillis)))
  ([^clojure.lang.IFn encoder ^String key ^String value ^Long timestamp]
     (-> (Column.)
         (.setName (encoder (name key)))
         (.setValue (encode value))
         (.setTimestamp timestamp))))