(ns clojurewerkz.cassaforte.thrift.core
  (:refer-clojure :exclude [get])
  (:use [clojurewerkz.support.string :only [to-byte-buffer]])
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.conversion :as c])
  (:import [org.apache.cassandra.thrift ColumnPath ColumnParent SlicePredicate SliceRange
            ColumnOrSuperColumn SuperColumn Mutation
            ]))

(defn to-column-vector
  [m]
  (map (fn [[key value]] (c/build-column key value)) m))

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
                   consistency-level)))

(defn batch-mutate-supercolumn
  [^String column-family ^String key ^String scolumn-name column-hash consistency-level]
  (let [super-column  (-> (SuperColumn.)
                          (.setName (to-byte-buffer scolumn-name))
                          (.setColumns (to-column-vector column-hash)))
        mutation      (-> (Mutation.)
                          (.setColumn_or_supercolumn (-> (ColumnOrSuperColumn.) (.setSuper_column super-column))))]
    (.batch_mutate cc/*cassandra-client*
                   (java.util.HashMap. {(to-byte-buffer key) {column-family [mutation]}})
                   consistency-level))

  )

(defn get
  [^String column-family ^String key ^String field consistency-level]
  (let [column-path (ColumnPath. column-family)]
    (.setColumn column-path (to-byte-buffer field))
    (.get cc/*cassandra-client*
          (to-byte-buffer key)
          column-path
          consistency-level)))

(defn get-slice
  ([column-family key consistency-level]
     (get-slice column-family key "" "" consistency-level))
  ([column-family key slice-start slice-finish consistency-level]
      (let [column-parent (ColumnParent. column-family)
            range         (-> (SliceRange.)
                              (.setStart (to-byte-buffer slice-start))
                              (.setFinish (to-byte-buffer slice-finish)))
            predicate     (-> (SlicePredicate.)
                              (.setSlice_range range))]
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
