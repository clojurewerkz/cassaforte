(ns clojurewerkz.cassaforte.query
  (:require [qbits.hayt :as hayt]
            [qbits.hayt.utils :as utils]
            [qbits.hayt.cql :as cql]
            [qbits.hayt.fns :as cql-fn]))

;;
;; Renderers
;;

(defn ->raw
  "Execute raw query"
  [query]
  (binding [cql/*prepared-statement* false]
    (cql/emit-query query)))

(defn ->prepared
  "Compile a query into a vector composed of the prepared string
  query and a vector of parameters."
  [query]
  (binding [cql/*prepared-statement* true
            cql/*param-stack* (atom [])]
    [(cql/emit-query query)
     @cql/*param-stack*]))

;;
;; Queries
;;

(defn select-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* where
* order-by
* limit
* table (optionaly using composition)"
  [table & clauses]
  (into {:select table :columns :*} clauses))

(defn insert-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#insertStmt

Takes a table identifier and additional clause arguments:
* using
* table (optionaly using composition)"
  [table values & clauses]
  (into {:insert table
         :values values} clauses))

(defn update-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#updateStmt

Takes a table identifier and additional clause arguments:

* using
* set-columns
* where
* table (optionaly using composition)"
  [table set-columns & clauses]
  (into {:update table
         :set-columns set-columns} clauses))

(defn delete-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#deleteStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* using
* where
* table (optionaly using composition)"
  [table & clauses]
  (into {:delete table :columns (keyword "")} clauses))

(defn truncate-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#truncateStmt

Takes a table identifier."
  [table]
  {:truncate table})

(defn drop-keyspace-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#dropKeyspaceStmt

Takes a keyspace identifier"
  [keyspace]
  {:drop-keyspace keyspace})

(defn drop-table-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#dropTableStmt

Takes a table identifier"
  [table]
  {:drop-table table})

(defn drop-index-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#dropIndexStmt

Takes an index identifier."
  [index]
  {:drop-index index})

(defn create-index-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#createIndexStmt

Takes a table identifier and additional clause arguments:

* index-column
* index-name
* table (optionaly using composition)"
  [table name & clauses]
  (into {:create-index name :on table} clauses))

(defn create-keyspace-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#createKeyspaceStmt

Takes a keyspace identifier and clauses:
* with
* keyspace (optionaly using composition)"
  [keyspace & clauses]
  (into {:create-keyspace keyspace} clauses))

(defn create-table-query
  "Takes a table identifier and additional clause arguments:

* column-definitions
* with"
  [table & clauses]
  (into {:create-table table} clauses))

(defn alter-table-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterTableStmt

Takes a table identifier and additional clause arguments:

* alter-column
* add
* with
* alter
* rename"
  [table & clauses]
  (into {:alter-table table} clauses))

(defn alter-column-family-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterTableStmt

Takes a column-familiy identifier and additional clause arguments:

* alter-column
* add
* with
* alter
* rename
* column-family (optionaly using composition)"
  [column-family & clauses]
  (into {:alter-column-family column-family} clauses))

(defn alter-keyspace-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterKeyspaceStmt

Takes a keyspace identifier and a `with` clause.
* keyspace (optionaly using composition)"
  [keyspace & clauses]
  (into {:alter-keyspace keyspace} clauses))

(defn batch-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#batchStmt

Takes hayt queries  optional clauses:
* using
* counter
* logged "
  [& clauses]
  (into {:logged true} clauses))

(defn use-keyspace-query
  "http://cassandra.apache.org/doc/cql3/CQL.html#useStmt

Takes a keyspace identifier"
  [keyspace]
  {:use-keyspace keyspace})

(defn grant-query
  "Takes clauses:
* resource
* user"
  [perm & clauses]
  (into {:grant perm} clauses))

(defn revoke-query
  "Takes clauses:
* resource
* user"
  [perm & clauses]
  (into {:revoke perm} clauses))

(defn create-user-query
  "Takes clauses:
* password
* superuser (defaults to false)"
  [user & clauses]
  (into {:create-user user :superuser false} clauses))

(defn alter-user-query
  "Takes clauses:
* password
* superuser (defaults to false)"
  [user & clauses]
  (into {:alter-user user :superuser false} clauses))

(defn drop-user-query
  [user]
  {:drop-user user})

(defn list-users-query
  []
  {:list-users nil})

(defn list-perm-query
  "Takes clauses:
* perm (defaults to ALL if not supplied)
* user
* resource
* recursive (defaults to true)"
  [& clauses]
  (into {:list-perm :ALL :recursive true} clauses))

;;
;; Clauses
;;

(defn columns
  "Clause: takes columns identifiers"
  [& columns]
  {:columns columns})

(defn column-definitions
  "Clause: "
  [column-definitions]
  {:column-definitions column-definitions})

(defn using
  "Clause: takes keyword/value pairs for :timestamp and :ttl"
  [& args]
  {:using (apply hash-map args)})

(defn limit
  "Clause: takes a numeric value"
  [n]
  {:limit n})

(defn order-by
  "Clause: takes vectors of 2 elements, where the first is the column
  identifier and the second is the ordering as keyword.
  ex: :asc, :desc"
  [& columns] {:order-by columns})

(defn queries
  "Clause: takes hayt queries to be executed during a batch operation."
  [& queries]
  {:batch queries})

(defn where
  "Clause: takes a map or a vector of pairs to compose the where
clause of a select/update/delete query"
  [& args]
  {:where (partition 2 args)})

(defn values
  "Clause: Takes a map of columns to be inserted"
  [values]
  {:values values})

(defn set-columns
  "Clause: Takes a map of columns to be updated"
  [values]
  {:set-columns values})

(defn with
  "Clause: compiles to a CQL with clause (possibly nested maps)"
  [values]
  {:with values})

(defn index-name
  "Clause: Takes an index identifier"
  [value]
  {:index-name value})

(defn alter-column
  "Clause: takes a table identifier and a column type"
  [& args]
  {:alter-column args})

(defn add-column
  "Clause: takes a table identifier and a column type"
  [& args]
  {:add-column args})

(defn rename-column
  "Clause: rename from old-name to new-name"
  [& args]
  {:rename-column args})

(defn drop-column
  "Clause: Takes a column Identifier"
  [id]
  {:drop-column id})

(defn allow-filtering
  "Clause: sets ALLOW FILTERING on select queries, defaults to true is
   used without a value"
  ([value]
     {:allow-filtering value})
  ([]
     (allow-filtering true)))

(defn logged
  "Clause: Sets LOGGED/UNLOGGED attribute on BATCH queries"
  ([value]
     {:logged value})
  ([]
     (logged true)))

(defn counter
  "Clause: Sets COUNTER attribute on BATCH queries"
  ([value]
     {:counter value})
  ([]
     (counter true)))

(defn superuser
  "Clause: To be used with alter-user and create-user, sets superuser status"
  ([value]
     {:superuser value})
  ([]
     (superuser true)))

(defn password
  "Clause: To be used with alter-user and create-user, sets password"
  [value]
  {:password value})

(defn recursive
  "Clause: Sets recusivity on list-perm (LIST PERMISSION) queries"
  ([value]
     {:recursive value})
  ([]
     (recursive true)))

(defn resource
  "Clause: Sets resource to be modified/used with grant or list-perm"
  [value]
  {:resource value})

(defn user
  "Clause: Sets user to be modified/used with grant or list-perm"
  [value]
  {:user value})

(defn perm
  "Clause: Sets permission to be listed with list-perm"
  [value]
  {:list-perm value})

(defn custom
  "Clause: Sets CUSTOM status on create-index query"
  ([x]
     {:custom x})
  ([]
     (custom true)))

;;
;;
;;

(def now
  "Returns a now() CQL function"
  (constantly (cql-fn/cql-fn "now")))

(def count*
  "Returns a count(*) CQL function"
  (constantly (cql-fn/cql-fn "COUNT" :*)))

(def count1
  "Returns a count(1) CQL function"
  (constantly (cql-fn/cql-fn "COUNT" 1)))

;;
;;
;;

(def map-type (comp keyword utils/map-type))
(def list-type (comp keyword utils/list-type))
(def set-type (comp keyword utils/set-type))
