;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns clojurewerkz.cassaforte.query
  "Functions for building dynamic CQL queries, in case you feel
   that `cql` namespace is too limiting for you."
  (:require [qbits.hayt.dsl.statement :as statement]
            [qbits.hayt.cql :as cql]
            [clojurewerkz.cassaforte.aliases :refer :all]))

(doseq [module '(dsl.clause fns utils)]
  (alias-ns (symbol (str "qbits.hayt." module))))

;;
;; Cassaforte Mods
;;

(def select-query statement/select)

(defn insert-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#insertStmt

Takes a table identifier and additional clause arguments:
* values
* using"
  [table values & clauses]
  (into {:insert table
         :values values} clauses))

(defn update-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#updateStmt

Takes a table identifier and additional clause arguments:

* using
* set-columns
* where
* only-if
* if-not-exists"
  [table set-columns & clauses]
  (into {:update table
         :set-columns set-columns} clauses))

(defalias delete-query statement/delete)

(defalias truncate-query statement/truncate)
(defalias drop-keyspace-query statement/drop-keyspace)
(defalias drop-table-query statement/drop-table)
(defalias drop-index-query statement/drop-index)
(defalias create-index-query statement/create-index)

(defalias create-keyspace-query  statement/create-keyspace)
(defalias create-table-query  statement/create-table)
(defalias create-column-family-query  statement/create-table)
(defalias alter-table-query  statement/alter-table)
(defalias alter-column-family-query  statement/alter-column-family)
(defalias alter-keyspace-query  statement/alter-keyspace)

(defalias batch-query  statement/batch)

(defalias use-keyspace-query  statement/use-keyspace)

(defalias grant-query  statement/grant)
(defalias revoke-query  statement/revoke)
(defalias create-user-query statement/create-user)
(defalias alter-user-query statement/alter-user)
(defalias drop-user-query statement/drop-user)
(defalias list-users-query statement/list-users)
(defalias list-perm-query statement/list-perm)

;;
;; Overrides
;;

(defn- transform-ranges
  [coll]
  (map (fn [[k v]]
         (if (sequential? v)
           [(first v) k (second v)]
           [k v]))
       coll))

(defn where
  "Clause: takes a map or a vector of pairs to compose the where
clause of a select/update/delete query"
  [& args]
  (if (and (= 1 (count args)) (-> args first map?))
    {:where (transform-ranges (first args))}
    {:where (->> (partition 2 args)
                 (transform-ranges))}))

(defn paginate
  "Paginate through the collection of results

   Params:
     * `where` - where query to lock a partition key
     * `key` - key to paginate on
     * `last-key` - last seen value of the key, next chunk of results will contain all keys that follow that value
     * `per-page` - how many results per page"
  ([& {:keys [key last-key per-page where] :or {:page 0}}]
     {:limit per-page
      :where (if last-key
               (assoc where key [> last-key])
               where)}))


(defn increment-by
  [num]
  [+ num])

(defn decrement-by
  [num]
  [- num])
