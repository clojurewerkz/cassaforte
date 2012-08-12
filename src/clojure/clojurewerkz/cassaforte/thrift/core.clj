(ns clojurewerkz.cassaforte.thrift.core
  (:refer-clojure :exclude [get])
  (:use [clojurewerkz.support.string :only [to-byte-buffer]])
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.conversion :as c])
  (:import [org.apache.cassandra.thrift ColumnPath]))

(defn to-mutation
  [m]
  (map (fn [[key value]] (c/build-mutation (c/build-column key value))) m))

(defn- apply-to-values [m f]
  "Applies function f to all values in map m"
  (into {} (for [[k v] m]
             [k (f v)])))

(defn batch-mutate
  [mutation-map consistency-level]
  (let [keys             (map to-byte-buffer (keys mutation-map))
        mutations        (map #(apply-to-values % to-mutation) (vals mutation-map))
        batch-mutate-map (zipmap keys mutations)]
    (.batch_mutate cc/*cassandra-client*
                   (java.util.HashMap. batch-mutate-map)
                   consistency-level)))>


(defn get
  [^String key ^String column-family ^String field consistency-level]
  (let [column-path (ColumnPath. column-family)]
    (.setColumn column-path (to-byte-buffer field))
    (.get cc/*cassandra-client*
          (to-byte-buffer key)
          column-path
          consistency-level)))

;; get-count
;; insert
;; get-slice
;; multiget-count
;; multiget-slice
;; get-range-slices
;; get-paged-slice
;; get-indexed-slices
;; add
;; remove
;; remove-counter
