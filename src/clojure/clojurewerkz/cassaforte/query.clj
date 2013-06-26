(ns clojurewerkz.cassaforte.query
  "Functions for building dynamic CQL queries, in case you feel
   that `cql` namespace is too limiting for you."
  (:require
   [clojurewerkz.cassaforte.ns-utils :as ns-utils]
   [qbits.hayt.dsl.statement :as statement]
   [qbits.hayt.cql :as cql]))

(doseq [module '(dsl.clause fns utils)]
  (ns-utils/alias-ns (symbol (str "qbits.hayt." module))))

;;
;; Cassaforte Mods
;;


(def select-query statement/select)

(defn insert-query
  [table values & clauses]
  (into {:insert table
         :values values} clauses))

(defn update-query
  [table set-columns & clauses]
  (into {:update table
         :set-columns set-columns} clauses))

(def delete-query statement/delete)

(def truncate-query statement/truncate)
(def drop-keyspace-query statement/drop-keyspace)
(def drop-table-query statement/drop-table)
(def drop-index-query statement/drop-index)
(def create-index-query statement/create-index)

(def create-keyspace-query statement/create-keyspace)
(def create-table-query statement/create-table)
(def create-column-family-query statement/create-table)
(def alter-table-query statement/alter-table)
(def alter-column-family-query statement/alter-column-family)
(def alter-keyspace-query statement/alter-keyspace)

(def batch-query statement/batch)

(def use-keyspace-query statement/use-keyspace)

(def grant-query statement/grant)
(def revoke-query statement/revoke)
(def create-user-query statement/create-user)
(def alter-user-query statement/alter-user)
(def drop-user-query statement/drop-user)
(def list-users-query statement/list-users)
(def list-perm-query statement/list-perm)

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
