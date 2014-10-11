;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns clojurewerkz.cassaforte.cql
  "Main namespace for working with CQL, prepared statements. Convenience functions
   for key operations built on top of CQL."
  (:refer-clojure :exclude [update])
  (:require [qbits.hayt.cql :as hayt]
            [clojurewerkz.cassaforte.query :as q]
            [clojurewerkz.cassaforte.client :as cc])
  (:import com.datastax.driver.core.Session))

(defn ^:private execute-
  [^Session session query-params builder]
  (let [rendered-query (cc/render-query (cc/compile-query query-params builder))]
    (cc/execute session rendered-query {:prepared hayt/*prepared-statement*})))

(defn ^:private execute-async-
  [^Session session query-params builder]
  (let [rendered-query (cc/render-query (cc/compile-query query-params builder))]
    (cc/execute-async session rendered-query {:prepared hayt/*prepared-statement*})))

;;
;; Schema operations
;;

(defn drop-keyspace
  "Drops a keyspace: results in immediate, irreversible removal of an existing keyspace,
   including all column families in it, and all data contained in those column families."
  [^Session session ks]
  (execute- session [ks] q/drop-keyspace-query))

(defn create-keyspace
  "Creates a new top-level keyspace. A keyspace is a namespace that
   defines a replication strategy and some options for a set of tables,
   similar to a database in relational databases.

   Example:

     (create-keyspace conn :new_cql_keyspace
                   (with {:replication
                          {:class \"SimpleStrategy\"
                           :replication_factor 1}}))"
  [^Session session & query-params]
  (execute- session query-params q/create-keyspace-query))

(defn create-index
  "Creates a new (automatic) secondary index for a given (existing)
   column in a given table.If data already exists for the column, it will be indexed during the execution
   of this statement. After the index is created, new data for the column is indexed automatically at
   insertion time.

   Example, creates an index on `users` table, `city` column:

      (create-index conn :users :city
                    (index-name :users_city)
                    (if-not-exists))"
  [^Session session & query-params]
  (execute- session query-params q/create-index-query))

(defn drop-index
  "Drop an existing secondary index. The argument of the statement
   is the index name.

   Example, drops an index on `users` table, `city` column:

       (drop-index th/session :users_city)"
  [^Session session & query-params]
  (execute- session query-params q/drop-index-query))

(defn create-table
  "Creates a new table. A table is a set of rows (usually
   representing related entities) for which it defines a number of properties.

   A table is defined by a name, it defines the columns composing rows
   of the table and have a number of options.

   Example:

     (create-table :users
                (column-definitions {:name :varchar
                                     :age  :int
                                     :city :varchar
                                     :primary-key [:name]}))"
  [^Session session & query-params]
  (execute- session query-params q/create-table-query))

(def create-column-family create-table)

(defn drop-table
  "Drops a table: this results in the immediate, irreversible removal of a table, including
   all data in it."
  [^Session session ks]
  (execute- session [ks] q/drop-table-query))

(defn use-keyspace
  "Takes an existing keyspace name as argument and set it as the per-session current working keyspace.
   All subsequent keyspace-specific actions will be performed in the context of the selected keyspace,
   unless otherwise specified, until another USE statement is issued or the connection terminates."
  [^Session session ks]
  (execute- session [ks] q/use-keyspace-query))

(defn alter-table
  "Alters a table definition. Use it to add new
   columns, drop existing ones, change the type of existing columns, or update the table options."
  [^Session session & query-params]
  (execute- session query-params q/alter-table-query))

(defn alter-keyspace
  "Alters properties of an existing keyspace. The
   supported properties are the same that for `create-keyspace`"
  [^Session session & query-params]
  (execute- session query-params q/alter-keyspace-query))

;;
;; DB Operations
;;

(defn insert
  "Inserts a row in a table.

   Note that since a row is identified by its primary key, the columns that compose it must be
   specified. Also, since a row only exists when it contains one value for a column not part of
   the primary key, one such value must be specified too."
  [^Session session & query-params]
  (execute- session query-params q/insert-query))

(defn insert-async
  "Same as insert but returns a future"
  [^Session session & query-params]
  (execute-async- session query-params q/insert-query))

(defn ^{:private true} batch-query-from
  [table records]
  (->> records
       (map (comp (partial apply (partial q/insert-query table)) flatten vector))
       (apply q/queries)
       q/batch-query
       cc/render-query))

(defn insert-batch
  "Performs a batch insert (inserts multiple records into a table at the same time).
  To specify additional clauses for a record (such as where or using), wrap that record
  and the clauses in a vector"
  [^Session session table records]
  (let [query (batch-query-from table records)]
    (cc/execute session query {:prepared hayt/*prepared-statement*})))

(defn insert-batch-async
  "Same as insert-batch but returns a future"
  [^Session session table records]
  (let [query (batch-query-from table records)]
    (cc/execute-async session query {:prepared hayt/*prepared-statement*})))

(defn update
  "Updates one or more columns for a given row in a table. The `where` clause
   is used to select the row to update and must include all columns composing the PRIMARY KEY.
   Other columns values are specified through assignment within the `set` clause."
  [^Session session & query-params]
  (execute- session query-params q/update-query))

(defn update-async
  "Same as update but returns a future"
  [^Session session & query-params]
  (execute-async- session query-params q/update-query))

(defn delete
  "Deletes columns and rows. If the `columns` clause is provided,
   only those columns are deleted from the row indicated by the `where` clause, please refer to
   doc guide (http://clojurecassandra.info/articles/kv.html) for more details. Otherwise whole rows
   are removed. The `where` allows to specify the key for the row(s) to delete. First argument
   for this function should always be table name."
  [^Session session table & query-params]
  (execute- session (cons table query-params) q/delete-query))

(defn delete-async
  "Same as delete but returns a future"
  [^Session session table & query-params]
  (execute-async- session (cons table query-params) q/delete-query))

(defn select
  "Retrieves one or more columns for one or more rows in a table.
   It returns a result set, where every row is a collection of columns returned by the query."
  [^Session session & query-params]
  (execute- session query-params q/select-query))

(defn select-async
  "Same as select but returns a future"
  [^Session session & query-params]
  (execute-async- session query-params q/select-query))

(defn truncate
  "Truncates a table: permanently and irreversably removes all rows from the table,
   not removing the table itself."
  [^Session session table]
  (execute- session [table] q/truncate-query))

(defn create-user
  [^Session session & query-params]
  (execute- session query-params q/create-user-query))

(defn alter-user
  [^Session session & query-params]
  (execute- session query-params q/alter-user-query))

(defn drop-user
  [^Session session & query-params]
  (execute- session query-params q/drop-user-query))

(defn grant
  [^Session session & query-params]
  (execute- session query-params q/grant-query))

(defn revoke
  [^Session session & query-params]
  (execute- session query-params q/revoke-query))

(defn list-users
  [^Session session & query-params]
  (execute- session query-params q/list-users-query))

(defn list-permissions
  [^Session session & query-params]
  (execute- session query-params q/list-perm-query))

;;
;; Higher level DB functions
;;

(defn get-one
  "Executes query to get exactly one result. Does not add `limit` clause to the query, this is
   a convenience function only. Please use `limit` clause if you execute queries that potentially
   return more than a single result."
  [^Session session & query-params]
  (first (execute- session query-params q/select-query)))

(defn perform-count
  "Helper function to perform count on a table with given query. Count queries are slow in Cassandra,
   in order to get a rough idea of how many items you have in certain table, use `nodetool cfstats`,
   for more complex cases, you can wither do a full table scan or perform a count with this function,
   please note that it does not have any performance guarantees and is potentially expensive."
  [^Session session table & query-params]
  (:count
   (first
    (select session table
            (cons
             (q/columns (q/count*))
             query-params)))))

;;
;; Higher-level helper functions for schema
;;

(defn describe-keyspace
  "Returns a keyspace description, taken from `system.schema_keyspaces`."
  [^Session session ks]
  (first
   (select session :system.schema_keyspaces
           (q/where {:keyspace_name (name ks)}))))

(defn describe-table
  "Returns a table description, taken from `system.schema_columnfamilies`."
  [^Session session ks table]
  (first
   (select session :system.schema_columnfamilies
           (q/where {:keyspace_name (name ks)
                     :columnfamily_name (name table)}))))

(defn describe-columns
  "Returns table columns description, taken from `system.schema_columns`."
  [^Session session ks table]
  (select session :system.schema_columns
          (q/where {:keyspace_name (name ks)
                    :columnfamily_name (name table)})))

;;
;; Higher-level collection manipulation
;;

(defn- load-chunk
  "Returns next chunk for the lazy table iteration"
  [^Session session table partition-key chunk-size last-pk]
  (if (nil? (first last-pk))
    (select session table
            (q/limit chunk-size))
    (select session table
            (q/where [[> (apply q/token partition-key) (apply q/token last-pk)]])
            (q/limit chunk-size))))

(defn iterate-table
  "Lazily iterates through a table, returning chunks of chunk-size."
  ([^Session session table partition-key chunk-size]
     (iterate-table session table (if (sequential? partition-key)
                                    partition-key
                                    [partition-key])
                    chunk-size []))
  ([^Session session table partition-key chunk-size c]
     (lazy-cat c
               (let [last-pk    (map #(get (last c) %) partition-key)
                     next-chunk (load-chunk session table partition-key chunk-size last-pk)]
                 (if (empty? next-chunk)
                   []
                   (iterate-table session table partition-key chunk-size next-chunk))))))
