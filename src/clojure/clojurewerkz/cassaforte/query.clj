(ns clojurewerkz.cassaforte.query
  (:require
   [flatland.useful.ns :as uns]
   [qbits.hayt.dsl.verb :as verb]
   [qbits.hayt.cql :as cql]))

(def ->raw
  "Compiles a hayt query into its raw string value"
  cql/->raw)

(def ->prepared
  "Compiles a hayt query into a vector composed of the prepared string
  query and a vector of parameters."
  cql/->prepared)

(doseq [module '(dsl.clause fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))

;;
;; Cassaforte Mods
;;


(def select-query verb/select)

(defn insert-query
  [table values & clauses]
  (into {:insert table
         :values values} clauses))

(defn update-query
  [table set-columns & clauses]
  (into {:update table
         :set-columns set-columns} clauses))

(def delete-query verb/delete)

(def truncate-query verb/truncate)
(def drop-keyspace-query verb/drop-keyspace)
(def drop-table-query verb/drop-table)
(def drop-index-query verb/drop-index)
(def create-index-query verb/create-index)

(def create-keyspace-query verb/create-keyspace)
(def create-table-query verb/create-table)
(def create-column-family-query verb/create-table)
(def alter-table-query verb/alter-table)
(def alter-column-family-query verb/alter-column-family)
(def alter-keyspace-query verb/alter-keyspace)

(def batch-query verb/batch)

(def use-keyspace-query verb/use-keyspace)

(def grant-query verb/grant)
(def revoke-query verb/revoke)
(def create-user-query verb/create-user)
(def alter-user-query verb/alter-user)
(def drop-user-query verb/drop-user)
(def list-users-query verb/list-users)
(def list-perm-query verb/list-perm)

(defn where
  "Clause: takes a map or a vector of pairs to compose the where
clause of a select/update/delete query"
  [& args]
  {:where (partition 2 args)})

(defn paginate
  ([& {:keys [key last-key per-page where] :or {:page 0}}]
     {:limit per-page
      :where (if last-key
               (assoc where key [> last-key])
               where)}))
