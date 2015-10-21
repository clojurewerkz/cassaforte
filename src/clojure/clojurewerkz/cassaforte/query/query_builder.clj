(ns clojurewerkz.cassaforte.query.query-builder
  "Functions for building dynamic CQL queries, in case you feel
   that `cql` namespace is too limiting for you."
  (:refer-clojure :exclude [update])
  (:import [com.datastax.driver.core.querybuilder QueryBuilder]))

;;
;; Query Builder helper methods
;;

(defn bind-marker
  [name]
  (QueryBuilder/bindMarker name))

(defn timestamp
  [column-name]
  (QueryBuilder/timestamp column-name))

(defn token
  [& column-names]
  (QueryBuilder/token (into-array (map name column-names))))

(defn function-call ;; Maybe rename to raw-function-call?
  [name & args]
  (QueryBuilder/fcall name (object-array args)))

(defn now
  []
  (function-call "now"))

(defn min-timeuuid
  [v]
  (function-call "minTimeuuid" v))

(defn max-timeuuid
  [v]
  (function-call "maxTimeuuid" v))

(defn asc
  [^String column-name]
  (QueryBuilder/asc (name column-name)))

(defn desc
  [^String column-name]
  (QueryBuilder/desc (name column-name)))

(defn cname
  [^String column-name]
  (QueryBuilder/column column-name))

(defn quote*
  [s]
  (QueryBuilder/quote (name s)))

;;
;; Assignments
;;

(defn set-column
  [^String column-name column-value]
  (QueryBuilder/set column-name column-value))

(defn increment
  []
  (fn [column-name]
    (QueryBuilder/incr column-name)))

(defn increment-by
  [by-value]
  (fn [column-name]
    (QueryBuilder/incr column-name by-value)))

(defn decrement
  []
  (fn [column-name]
    (QueryBuilder/decr column-name)))

(defn decrement-by
  [by-value]
  (fn [column-name]
    (QueryBuilder/decr column-name by-value)))

(defn prepend
  [value]
  (fn [column-name]
    (QueryBuilder/prepend column-name value)))

(defn prepend-all
  [values]
  (fn [column-name]
    (QueryBuilder/prependAll column-name values)))

(defn append
  [value]
  (fn [column-name]
    (QueryBuilder/append column-name value)))

(defn append-all
  [values]
  (fn [column-name]
    (QueryBuilder/appendAll column-name values)))

(defn discard
  [value]
  (fn [column-name]
    (QueryBuilder/discard column-name value)))

(defn discard-all
  [values]
  (fn [column-name]
    (QueryBuilder/discardAll column-name values)))

(defn set-idx
  [idx value]
  (fn [column-name]
    (QueryBuilder/setIdx column-name idx value)))

(defn add-tail
  [value]
  (fn [column-name]
    (QueryBuilder/add column-name value)))

(defn add-all-tail
  [values]
  (fn [column-name]
    (QueryBuilder/addAll column-name values)))

(defn remove-tail
  [value]
  (fn [column-name]
    (QueryBuilder/remove column-name value)))

(defn remove-all-tail
  [values]
  (fn [column-name]
    (QueryBuilder/removeAll column-name values)))

(defn put-value
  [key value]
  (fn [column-name]
    (QueryBuilder/put column-name key value)))

(defn put-values
  [values]
  (fn [column-name]
    (QueryBuilder/putAll column-name values)))

(def ? (QueryBuilder/bindMarker))
