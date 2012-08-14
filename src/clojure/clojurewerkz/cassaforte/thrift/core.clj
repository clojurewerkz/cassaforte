(ns clojurewerkz.cassaforte.thrift.core
  (:refer-clojure :exclude [get])
  (:use [clojurewerkz.support.string :only [to-byte-buffer]])
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.conversion :as c]))

(defn- batch-mutate-transform
  [m type]
  (map (fn [[key value]] (c/build-mutation
                          (if (= type :super)
                            (c/build-super-column (name key) value)
                            (c/build-column key value))))
       m))

(defn- apply-to-values [m f]
  "Applies function f to all values in map m"
  (into {} (for [[k v] m]
             [k (f v)])))

(defn batch-mutate
  [mutation-map consistency-level & {:keys [type] :or {type :column}}]
  (let [keys             (map to-byte-buffer (keys mutation-map))
        mutations        (map #(apply-to-values % (fn [x] (batch-mutate-transform x type))) (vals mutation-map))
        batch-mutate-map (zipmap keys mutations)]
    (.batch_mutate cc/*cassandra-client*
                   (java.util.HashMap. batch-mutate-map)
                   consistency-level)))

(defn get
  [^String column-family ^String key ^String field consistency-level & {:keys [type] :or {type :column}}]
  (let [column-path (c/build-column-path column-family field type)]
    (.get cc/*cassandra-client*
          (to-byte-buffer key)
          column-path
          consistency-level)))

(defn get-slice
  ([column-family key consistency-level]
     (get-slice column-family key "" "" consistency-level))
  ([column-family key slice-start slice-finish consistency-level]
      (let [column-parent (c/build-column-parent column-family)
            range         (c/build-slice-range slice-start slice-finish)
            predicate     (c/build-slice-predicate range)]
        (.get_slice cc/*cassandra-client*
                    (to-byte-buffer key)
                    column-parent
                    predicate
                    consistency-level))))


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
