(ns clojurewerkz.cassaforte.thrift.core
  (:refer-clojure :exclude [get])
  (:use [clojurewerkz.support.string :only [to-byte-buffer]])
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.conversion :as c])
  (:import [org.apache.cassandra.thrift ColumnPath ColumnParent SlicePredicate SliceRange
            ColumnOrSuperColumn SuperColumn Mutation
            ]))

(defn- batch-mutate-transform
  [m & {:keys [type] :or {type :column}}]
  (map (fn [[key value]] (c/build-mutation
                          (cond
                            (= type :column) (c/build-column key value)
                            (= type :super-column) (c/build-super-column (name key) value)
                            :else (throw (Exception. (str "Don't know type " type ", can build only from :column and :super-column"))))))
       m))

(defn- apply-to-values [m f]
  "Applies function f to all values in map m"
  (into {} (for [[k v] m]
             [k (f v)])))

(defn batch-mutate
  [mutation-map consistency-level]
  (let [keys             (map to-byte-buffer (keys mutation-map))
        mutations        (map #(apply-to-values % batch-mutate-transform) (vals mutation-map))
        batch-mutate-map (zipmap keys mutations)]
    (.batch_mutate cc/*cassandra-client*
                   (java.util.HashMap. batch-mutate-map)
                   consistency-level)))

(defn batch-mutate-supercolumns
  [mutation-map consistency-level]
  (let [keys             (map to-byte-buffer (keys mutation-map))
        mutations        (map #(apply-to-values % (fn [x] (batch-mutate-transform x :type :super-column))) (vals mutation-map))
        batch-mutate-map (zipmap keys mutations)]
    (.batch_mutate cc/*cassandra-client*
                   (java.util.HashMap. batch-mutate-map)
                   consistency-level)))

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
