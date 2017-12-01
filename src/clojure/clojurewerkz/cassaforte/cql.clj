;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns clojurewerkz.cassaforte.cql
  "Main namespace for working with CQL, prepared statements. Convenience functions
  for key operations built on top of CQL."
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.query   :as query]
            [clojurewerkz.cassaforte.aliases :as alias]
            [clojurewerkz.cassaforte.client  :as cc])
  (:import com.datastax.driver.core.Session))

;;
;; Imports
;;

(alias/alias-ns 'clojurewerkz.cassaforte.query.query-builder)
(alias/alias-ns 'clojurewerkz.cassaforte.query.dsl)
(alias/alias-ns 'clojurewerkz.cassaforte.query.column)
(alias/alias-ns 'clojurewerkz.cassaforte.query.types)

;;
;; Schema operations
;;

(defn drop-keyspace
  "Drops a keyspace: results in immediate, irreversible removal of an existing keyspace,
  including all column families in it, and all data contained in those column families."
  [^Session session ks & query-params]
  (cc/execute session
              (apply query/drop-keyspace ks query-params)))

(defn create-keyspace
  "Creates a new top-level keyspace. A keyspace is a namespace that
  defines a replication strategy and some options for a set of tables,
  similar to a database in relational databases.

  Example:

      (create-keyspace session \"new_cql_keyspace\"
                       (with {:replication
                              {\"class\"              \"SimpleStrategy\"
                               \"replication_factor\" 1 }}))
  "
  [^Session session keyspace & query-params]
  (cc/execute session
              (apply query/create-keyspace (cons keyspace query-params))))

(defn create-index
  "Creates a new (automatic) secondary index for a given (existing)
  column in a given table.If data already exists for the column, it will be indexed during the execution
  of this statement. After the index is created, new data for the column is indexed automatically at
  insertion time.

  Example, creates an index named `users_city_idx' on `users` table, `city` column:

  (create-index *session* :users_city_idx
                (on-table :users
                (and-column :city)
                (if-not-exists)
  "
  [^Session session & query-params]
  (cc/execute session
              (apply query/create-index query-params)))

;; (defn drop-index
;;   "Drop an existing secondary index. The argument of the statement
;;    is the index name.

;;    Example, drops an index on `users` table, `city` column:

;;        (drop-index th/session :users_city)"
;;   [^Session session & query-params]
;;   (cc/execute session
;;               (apply query/drop-index query-params) ))

(defn create-table
  "Creates a new table. A table is a set of rows (usually
  representing related entities) for which it defines a number of properties.

  A table is defined by a name, it defines the columns composing rows
  of the table and have a number of options.

  Example:

  (create-table *session* :userstmp
                (column-definitions {:name        :varchar
                                     :title       :varchar
                                     :primary-key [:name]}))
  "
  [^Session session & query-params]
  (cc/execute session
              (apply query/create-table query-params)))

(def create-column-family create-table)

(defn drop-table
  "Drops a table: this results in the immediate, irreversible removal of a table, including
  all data in it."
  [^Session session ks]
  (cc/execute session
              (query/drop-table ks)))

(defn use-keyspace
  "Takes an existing keyspace name as argument and set it as the per-session current working keyspace.
  All subsequent keyspace-specific actions will be performed in the context of the selected keyspace,
  unless otherwise specified, until another USE statement is issued or the connection terminates."
  [^Session session ks]
  (cc/execute session
              (query/use-keyspace ks)))

(defn alter-table
  "Alters a table definition. Use it to add new
   columns, drop existing ones, change the type of existing columns, or update the table options.

   Example:

   (alter-table *session* :people
                (rename-column :naome :name)
                (add-column :age :int))
  "
  [^Session session & query-params]
  (cc/execute session
              (apply query/alter-table query-params)))

(defn alter-keyspace
  "Alters properties of an existing keyspace. The
  supported properties are the same that for `create-keyspace`

   Example:

  (alter-keyspace *session* \"new_cql_keyspace\"
                  (with {:durable-writes false
                         :replication    {\"class\" \"NetworkTopologyStrategy\"
                                          \"dc1\"   1
                                          \"dc2\"   2}}))
  "
  [^Session session & query-params]
  (cc/execute session
              (apply query/alter-keyspace query-params)))

;;
;; DB Operations
;;

(defn insert
  "Inserts a row in a table.

  Note that since a row is identified by its primary key, the columns that compose it must be
  specified. Also, since a row only exists when it contains one value for a column not part of
  the primary key, one such value must be specified too.

   Example:

   (insert *session* :users
           {:name \"name1\"
            :age  (int 19)})
  "
  [^Session session & query-params]
  (cc/execute session
              (apply query/insert query-params)))


(defn insert-batch
  "Performs a batch insert (inserts multiple records into a table at the same time).
  To specify additional clauses for a record (such as where or using), wrap that record
  and the clauses in a vector"
  [^Session session table records]
  (cc/execute session
              (query/batch
               (apply
                query/queries
                (map #(query/insert table %) records)))))


(defn update
  "Updates one or more columns for a given row in a table. The `where` clause
   is used to select the row to update and must include all columns composing the PRIMARY KEY.
   Other columns values are specified through assignment within the `set` clause."
  [^Session session & query-params]
  (cc/execute session
              (apply query/update query-params)))

(defn delete
  "Deletes columns and rows. If the `columns` clause is provided,
   only those columns are deleted from the row indicated by the `where` clause, please refer to
   doc guide (http://clojurecassandra.info/articles/kv.html) for more details. Otherwise whole rows
   are removed. The `where` allows to specify the key for the row(s) to delete. First argument
   for this function should always be table name."
  [^Session session table & query-params]
  (cc/execute session
              (apply query/delete (cons table query-params))))

(defn select
  "Retrieves one or more columns for one or more rows in a table.
  It returns a result set, where every row is a collection of columns returned by the query."
  [^Session session & query-params]
  (cc/execute session
              (apply query/select query-params)))

(defn truncate
  "Truncates a table: permanently and irreversably removes all rows from the table,
  not removing the table itself."
  [^Session session table]
  (cc/execute session
              (query/truncate table)))

;; (defn create-user
;;   [^Session session & query-params]
;;   (cc/execute session
;;               (apply q/create-user-query query-params)))

;; (defn alter-user
;;   [^Session session & query-params]
;;   (cc/execute session
;;               (apply q/alter-user-query query-params)))

;; (defn drop-user
;;   [^Session session & query-params]
;;   (cc/execute session
;;               (apply q/drop-user-query query-params)))

;; (defn grant
;;   [^Session session & query-params]
;;   (cc/execute session
;;               (apply q/grant-query query-params)))

;; (defn revoke
;;   [^Session session & query-params]
;;   (cc/execute session
;;               (apply q/revoke-query query-params)))

;; (defn list-users
;;   [^Session session & query-params]
;;   (cc/execute session
;;               (apply q/list-users-query query-params)))

;; (defn list-permissions
;;   [^Session session & query-params]
;;   (cc/execute session
;;               (apply q/list-perm-query query-params)))

;;
;; Higher level DB functions
;;

(defn perform-count
  "Helper function to perform count on a table with given query. Count queries are slow in Cassandra,
  in order to get a rough idea of how many items you have in certain table, use `nodetool cfstats`,
  for more complex cases, you can wither do a full table scan or perform a count with this function,
  please note that it does not have any performance guarantees and is potentially expensive.

  Doesn't work as a prepared query."
  [^Session session table & query-params]
  (:count
   (first
    (apply select session table (query/count-all) query-params))))

;;
;; Higher-level collection manipulation
;;

(defn- load-chunk
  "Returns next chunk for the lazy table iteration"
  [^Session session table partition-key chunk-size last-pk]
  (if (nil? (first last-pk))
    (select session table
            (query/limit chunk-size))
    (select session table
            (query/where [[>
                           (apply query/token partition-key)
                           (apply query/function-call "token" last-pk)]])
            (query/limit chunk-size))))

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

(defn copy-table
  "Copies data from one table to another, transforming rows
  using provided function (`transform-fn`)"
  ([^Session session from-table to-table partition-key]
   (copy-table session from-table to-table partition-key 16384))
  ([^Session session from-table to-table partition-key chunk-size]
   (copy-table session from-table to-table partition-key identity chunk-size))
  ([^Session session from-table to-table partition-key transform-fn chunk-size]
   (doseq [row (iterate-table session from-table partition-key chunk-size)]
     (insert session to-table (transform-fn row)))))
