(ns clojurewerkz.cassaforte.thrift.core
  (:refer-clojure :exclude [get])
  (:use [clojurewerkz.cassaforte.thrift.query-builders]
        [clojurewerkz.cassaforte.conversion :as conv]
        [clojurewerkz.cassaforte.bytes :only [encode]])
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.ddl.column :as c]
            [clojurewerkz.cassaforte.ddl.super-column :as sc]
            [clojurewerkz.cassaforte.conversion :as conv]))

(defn- batch-mutate-transform
  [m type]
  (map (fn [[key value]] (build-mutation
                          (if (= type :super)
                            (sc/build-super-column (name key) value)
                            (c/build-column key value))))
       m))

(defn- apply-to-values [m f]
  "Applies function f to all values in map m"
  (into {} (for [[k v] m]
             [k (f v)])))

(defn batch-mutate
  [mutation-map & {:keys [type consistency-level]
                   :or {type :column
                        consistency-level (conv/to-consistency-level :one)}}]
  (let [keys             (map encode (keys mutation-map))
        mutations        (map #(apply-to-values % (fn [x] (batch-mutate-transform x type))) (vals mutation-map))
        batch-mutate-map (zipmap keys mutations)]

    (.batch_mutate client/*cassandra-client*
                   batch-mutate-map
                   consistency-level)))

(defn get
  [^String column-family ^String key ^String field & {:keys [type consistency-level]
                                                      :or {type :column
                                                           consistency-level (conv/to-consistency-level :one)}}]
  (let [column-path (build-column-path column-family field type)]
    (.get client/*cassandra-client*
          (encode key)
          column-path
          consistency-level)))

(defn- get-slice-raw*
  [column-family key slice-start slice-finish consistency-level]
  (let [column-parent (build-column-parent column-family)
        range         (build-slice-range slice-start slice-finish)
        predicate     (build-slice-predicate range)]
    (.get_slice client/*cassandra-client*
                (encode key)
                column-parent
                predicate
                consistency-level)))

(defn get-slice
  [column-family key & {:keys [slice-start slice-finish consistency-level schema]
                        :or { slice-start ""
                             slice-finish ""
                             consistency-level (conv/to-consistency-level :one)
                             schema {:default-value-type "UTF8Type" :default-name-type "UTF8Type"}}}]
  (let [slice (get-slice-raw* column-family key slice-start slice-finish consistency-level)]
    (conv/deserialize-thrift-response slice schema)))


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
