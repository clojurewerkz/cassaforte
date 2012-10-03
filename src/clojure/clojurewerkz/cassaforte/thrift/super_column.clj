(ns clojurewerkz.cassaforte.thrift.super-column
  (:use [clojurewerkz.cassaforte.bytes :only [encode]]
        [clojurewerkz.cassaforte.thrift.column :only [build-column]])
  (:import [org.apache.cassandra.thrift SuperColumn]))

;;
;; Getters
;;

(defn get-name
  [^SuperColumn cdef]
  (String. (.getName cdef)))

(defn get-columns
  [^SuperColumn cdef]
  (.getColumns cdef))

;;
;; Builders
;;

(defn build-super-column
  "Convert a clojure map to supercolumn"
  ([^String key ^clojure.lang.IPersistentMap column-map]
     (build-super-column key column-map (System/currentTimeMillis)))
  ([^String key ^clojure.lang.IPersistentMap column-map ^Long timestamp]
     (let [columns (map (fn [[key value]] (build-column key value timestamp)) column-map)]
       (doto (SuperColumn.)
         (.setName (encode key))
         (.setColumns columns)))))

(def build-sc build-super-column)