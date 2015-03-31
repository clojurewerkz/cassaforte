(ns clojurewerkz.cassaforte.new-query-api
  "Functions for building dynamic CQL queries, in case you feel
   that `cql` namespace is too limiting for you."
  (:import [com.datastax.driver.core.querybuilder QueryBuilder Select$Where Clause]
           [com.datastax.driver.core.querybuilder ]))



(defn where
  [query-builder m]

  )

(defn from
  [table-name]
  [2 (fn from-query [builder]
       (.from builder table-name))])

(defn all
  []
  [1 (fn all-query [^Select$Selection query-builder] (.all query-builder))])

(defn column
  [column]
  [1 (fn column-query [^Select$Selection query-builder]
       (.column query-builder column))])

(defn columns
  [columns]
  [1 (fn [^Select$Selection query-builder]
       (reduce (fn [builder column]
                 (.column builder column))
               query-builder
               columns))])

;; Clause eq(String name, Object value)
(defn- ^Clause eq
  [^String column ^Object value]
  (QueryBuilder/eq name value))

(defn- ^Clause in
  [^String column ^java.util.List values]
  (QueryBuilder/in name values))

(defn- ^Clause lt
  [^String column ^Object value]
  (QueryBuilder/lt name value))

(defn- ^Clause gt
  [^String column ^Object value]
  (QueryBuilder/gt name value))

(defn- ^Clause lte
  [^String column ^Object value]
  (QueryBuilder/lte name value))

(defn- ^Clause gte
  [^String column ^Object value]
  (QueryBuilder/gte name value))

(defprotocol WhereBuilder
  (build-where [construct query-builder]))

(extend-protocol WhereBuilder
  clojure.lang.IPersistentVector
  (build-where [construct query-builder]
    (reduce
     (fn [])
     query-builder)
    )
  clojure.lang.IPersistentMap
  (build-where [construct query-builder]
    ))

(defn where
  [m]
  [3 (fn where-query [query-builder]
       ()
       )]
  )

(defn select
  [table-name & statements]
  (let [query-builder (QueryBuilder/select)]
    (->> (conj statements (from table-name))
         (sort-by first)
         (map second)
         (reduce (fn [builder statement]
                   (statement builder))
                 query-builder
                 )
         (.toString))))



;; Select.Builder select(String... columns)
;; Select.Selection select()
;; Insert insertInto(String table)
;; Insert insertInto(String keyspace, String table)
;; Insert insertInto(TableMetadata table)
;; Update update(String table)
;; Update update(String keyspace, String table)
;; Update update(TableMetadata table)
;; Delete.Builder delete(String... columns)
;; Delete.Selection delete()
;; Batch batch(RegularStatement... statements)
;; Batch unloggedBatch(RegularStatement... statements)
;; Truncate truncate(String table)
;; Truncate truncate(String keyspace, String table)
;; Truncate truncate(TableMetadata table)
;; String quote(String columnName)
;; String token(String columnName)
;; String token(String... columnNames)
;; Ordering asc(String columnName)
;; Ordering desc(String columnName)
;; Using timestamp(long timestamp)
;; Using timestamp(BindMarker marker)
;; Using ttl(int ttl)
;; Using ttl(BindMarker marker)
;; Assignment set(String name, Object value)
;; Assignment incr(String name)
;; Assignment incr(String name, long value)
;; Assignment incr(String name, BindMarker value)
;; Assignment decr(String name)
;; Assignment decr(String name, long value)
;; Assignment decr(String name, BindMarker value)
;; Assignment prepend(String name, Object value)
;; Assignment prependAll(String name, List<?> list)
;; Assignment prependAll(String name, BindMarker list)
;; Assignment append(String name, Object value)
;; Assignment appendAll(String name, List<?> list)
;; Assignment appendAll(String name, BindMarker list)
;; Assignment discard(String name, Object value)
;; Assignment discardAll(String name, List<?> list)
;; Assignment discardAll(String name, BindMarker list)
;; Assignment setIdx(String name, int idx, Object value)
;; Assignment add(String name, Object value)
;; Assignment addAll(String name, Set<?> set)
;; Assignment addAll(String name, BindMarker set)
;; Assignment remove(String name, Object value)
;; Assignment removeAll(String name, Set<?> set)
;; Assignment removeAll(String name, BindMarker set)
;; Assignment put(String name, Object key, Object value)
;; Assignment putAll(String name, Map<?, ?> map)
;; Assignment putAll(String name, BindMarker map)
;; BindMarker bindMarker()
;; BindMarker bindMarker(String name)
;; Object raw(String str)
;; Object fcall(String name, Object... parameters)
;; Object column(String name)
