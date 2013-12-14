(ns clojurewerkz.cassaforte.cql
  "Main namespace for working with CQL, prepared statements. Convenience functions
   for key operations built on top of CQL."
  (:require
   [qbits.hayt.cql :as cql]
   [clojurewerkz.cassaforte.query :as query]
   [clojurewerkz.cassaforte.client :as client]))

(defn ^:private execute-
  [query-params builder]
  (let [rendered-query (client/render-query (client/compile-query query-params builder))]
    (client/execute client/*default-session* rendered-query :prepared cql/*prepared-statement*)))

;;
;; Schema operations
;;

(defn drop-keyspace
  "Drops a keyspace: results in immediate, irreversible removal of an existing keyspace,
   including all column families in it, and all data contained in those column families.

   Example:

     (drop-keyspace :new_cql_keyspace)"
  [ks]
  (execute- [ks] query/drop-keyspace-query))

(defn create-keyspace
  "Creates a new top-level keyspace. A keyspace is a namespace that
   defines a replication strategy and some options for a set of tables,
   similar to a database in relational databases.

   Example:

     (create-keyspace :new_cql_keyspace
                   (with {:replication
                          {:class \"SimpleStrategy\"
                           :replication_factor 1}}))"
  [& query-params]
  (execute- query-params query/create-keyspace-query))

(defn create-index
  "Creates a new (automatic) secondary index for a given (existing)
   column in a given table.If data already exists for the column, it will be indexed during the execution
   of this statement. After the index is created, new data for the column is indexed automatically at
   insertion time.

   Example, creates an index on `users` table, `city` column:

      (create-index th/session :users :city
                    (index-name :users_city)
                    (if-not-exists))"
  [& query-params]
  (execute- query-params query/create-index-query))

(defn drop-index
  "Drop an existing secondary index. The argument of the statement
   is the index name.

   Example, drops an index on `users` table, `city` column:

       (drop-index th/session :users_city)"
  [& query-params]
  (execute- query-params query/drop-index-query))

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
  [& query-params]
  (execute- query-params query/create-table-query))

(def create-column-family create-table)

(defn drop-table
  "Drops a table: this results in the immediate, irreversible removal of a table, including
   all data in it.

   Example:

     (drop-table :users)"
  [ks]
  (execute- [ks] query/drop-table-query))

(defn use-keyspace
  "Takes an existing keyspace name as argument and set it as the per-session current working keyspace.
   All subsequent keyspace-specific actions will be performed in the context of the selected keyspace,
   unless otherwise specified, until another USE statement is issued or the connection terminates.

   Example:

      (use :keyspace-name)"
  [ks]
  (execute- [ks] query/use-keyspace-query))

(defn alter-table
  "Alters a table definition. Use it to add new
   columns, drop existing ones, change the type of existing columns, or update the table options."
  [& query-params]
  (execute- query-params query/alter-table-query))

(defn alter-keyspace
  "Alters properties of an existing keyspace. The
   supported properties are the same that for `create-keyspace`"
  [& query-params]
  (execute- query-params query/alter-keyspace-query))

;;
;; DB Operations
;;

(defn insert
  "Inserts a row in a table.

   Note that since a row is identified by its primary key, the columns that compose it must be
   specified. Also, since a row only exists when it contains one value for a column not part of
   the primary key, one such value must be specified too."
  [& query-params]
  (execute- query-params query/insert-query))

(defn insert-batch
  "Performs a batch insert (inserts multiple records into a table at the same time).
  To specify additional clauses for a record (such as where or using), wrap that record
  and the clauses in a vector"
  [table records]
  (let [query (->> records
                   (map (comp (partial apply (partial query/insert-query table)) flatten vector))
                   (apply query/queries)
                   query/batch-query
                   client/render-query)]
    (client/execute client/*default-session* query :prepared cql/*prepared-statement*)))

(defn update
  "Updates one or more columns for a given row in a table. The `where` clause
   is used to select the row to update and must include all columns composing the PRIMARY KEY.
   Other columns values are specified through assignment within the `set` clause."
  [& query-params]
  (execute- query-params query/update-query))

(defn delete
  "Deletes columns and rows. If the `columns` clause is provided,
   only those columns are deleted from the row indicated by the `where` clause, please refer to
   KV guide (http://clojurecassandra.info/articles/kv.html) for more details. Otherwise whole rows
   are removed. The `where` allows to specify the key for the row(s) to delete. First argument
   for this function should always be table name.

   Example:

       (delete :users
               (where :name \"username\"))"
  [table & query-params]
  (execute- (cons table query-params) query/delete-query))

(defn select
  "Retrieves one or more columns for one or more rows in a table.
   It returns a result set, where every row is a collection of columns returned by the query."
  [& query-params]
  (execute- query-params query/select-query))

(defn truncate
  "Truncates a table: permanently and irreversably removes all rows from the table,
   not removing the table itself."
  [table]
  (execute- [table] query/truncate-query))

(defn create-user
  [& query-params]
  (execute- query-params query/create-user-query))

(defn alter-user
  [& query-params]
  (execute- query-params query/alter-user-query))

(defn drop-user
  [& query-params]
  (execute- query-params query/drop-user-query))

(defn grant
  [& query-params]
  (execute- query-params query/grant-query))

(defn revoke
  [& query-params]
  (execute- query-params query/revoke-query))

(defn list-users
  [& query-params]
  (execute- query-params query/list-users-query))

(defn list-permissions
  [& query-params]
  (execute- query-params query/list-perm-query))

;;
;; Higher level DB functions
;;

(defn get-one
  "Executes query to get exactly one result. Does not add `limit` clause to the query, this is
   a convenience function only. Please use `limit` clause if you execute queries that potentially
   return more than a single result."
  [& query-params]
  (first (execute- query-params query/select-query)))

(defn perform-count
  "Helper function to perform count on a table with given query. Count queries are slow in Cassandra,
   in order to get a rough idea of how many items you have in certain table, use `nodetool cfstats`,
   for more complex cases, you can wither do a full table scan or perform a count with this function,
   please note that it does not have any performance guarantees and is potentially expensive."
  [table & query-params]
  (:count
   (first
    (select table
            (cons
             (query/columns (query/count*))
             query-params)))))

;;
;; Higher-level helper functions for schema
;;

(defn describe-keyspace
  "Returns a keyspace description, taken from `system.schema_keyspaces`."
  [ks]
  (first
   (select :system.schema_keyspaces
           (query/where :keyspace_name ks))))

(defn describe-table
  "Returns a table description, taken from `system.schema_columnfamilies`."
  [ks table]
  (first
   (select :system.schema_columnfamilies
           (query/where :keyspace_name ks
                        :columnfamily_name table))))

(defn describe-columns
  "Returns table columns description, taken from `system.schema_columns`."
  [ks table]
  (select :system.schema_columns
          (query/where :keyspace_name ks
                       :columnfamily_name table)))

;;
;; Higher-level collection manipulation
;;

(defn- get-chunk
  "Returns next chunk for the lazy world iteration"
  [table partition-key chunk-size last-pk]
  (if (nil? (first last-pk))
    (select table
            (query/limit chunk-size))
    (select table
            (query/where (apply query/token partition-key) [> (apply query/token last-pk)])
            (query/limit chunk-size))))

(defn iterate-world
  "Lazily iterates through the collection, returning chunks of chunk-size."
  ([table partition-key chunk-size]
     (iterate-world table (if (sequential? partition-key)
                            partition-key
                            [partition-key])
                    chunk-size []))
  ([table partition-key chunk-size c]
     (lazy-cat c
               (let [last-pk    (map #(get (last c) %) partition-key)
                     next-chunk (get-chunk table partition-key chunk-size last-pk)]
                 (iterate-world table partition-key chunk-size next-chunk)))))
