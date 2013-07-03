(ns clojurewerkz.cassaforte.query
  "Functions for building dynamic CQL queries, in case you feel
   that `cql` namespace is too limiting for you."
  (:use [clojurewerkz.cassaforte.ns-utils])
  (:require
   [qbits.hayt.dsl.statement :as statement]
   [qbits.hayt.cql :as cql]))

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

(defn where
  "Clause: takes a map or a vector of pairs to compose the where
clause of a select/update/delete query"
  [& args]
  {:where (partition 2 args)})

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
