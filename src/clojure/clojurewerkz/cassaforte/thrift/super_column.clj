(ns clojurewerkz.cassaforte.thrift.super-column
  (:use    [clojurewerkz.support.string :only [to-byte-buffer]])
  (:use    [clojurewerkz.cassaforte.thrift.column :only [build-column]])
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
     (build-super-column to-byte-buffer key column-map (System/currentTimeMillis)))
  ([^clojure.lang.IFn encoder ^String key ^clojure.lang.IPersistentMap column-map ^Long timestamp]
     (let [columns (map (fn [[key value]] (build-column encoder key value timestamp)) column-map)]
       (-> (SuperColumn.)
           (.setName (encoder key))
           (.setColumns (java.util.ArrayList. columns))))))

(def build-sc build-super-column)