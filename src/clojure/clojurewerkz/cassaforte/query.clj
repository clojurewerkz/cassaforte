(ns clojurewerkz.cassaforte.query
  (:require
   [flatland.useful.ns :as uns]
   [qbits.hayt.cql :as cql]))

(def ->raw
  "Compiles a hayt query into its raw string value"
  cql/->raw)

(def ->prepared
  "Compiles a hayt query into a vector composed of the prepared string
  query and a vector of parameters."
  cql/->prepared)

(doseq [module '(dsl fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))

;;
;; Cassaforte Mods
;;

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

(defn using
  "Clause: takes keyword/value pairs for :timestamp and :ttl"
  [& args]
  {:using (apply hash-map args)})

(defn where
  "Clause: takes a map or a vector of pairs to compose the where
clause of a select/update/delete query"
  [& args]
  {:where (partition 2 args)})
