(ns clojurewerkz.cassaforte.new-query-api
  "Functions for building dynamic CQL queries, in case you feel
   that `cql` namespace is too limiting for you."
  (:import [com.datastax.driver.core.querybuilder QueryBuilder
            Select$Selection Select Select$Where
            BindMarker
            Clause]
           [com.datastax.driver.core.querybuilder ]))

;;
;; Static QB Methods
;;

(defn ?
  ([]
     (QueryBuilder/bindMarker))
  ([name]
     (QueryBuilder/bindMarker name)))

(defn token
  [& column-names]
  (QueryBuilder/token (into-array column-names)))

(def select-command-order
  [QueryBuilder Select$Selection Select Select$Where])




(defn- ^Clause eq
  [^String column ^Object value]
  (QueryBuilder/eq column value))

(defn- ^Clause in
  [^String column ^java.util.List values]
  (QueryBuilder/in column values))

(defn- ^Clause lt
  [^String column ^Object value]
  (QueryBuilder/lt column value))

(defn- ^Clause gt
  [^String column ^Object value]
  (QueryBuilder/gt column value))

(defn- ^Clause lte
  [^String column ^Object value]
  (QueryBuilder/lte column value))

(defn- ^Clause gte
  [^String column ^Object value]
  (QueryBuilder/gte column value))

(defn asc
  [^String column-name]
  (QueryBuilder/asc (name column-name)))

(defn desc
  [^String column-name]
  (QueryBuilder/desc (name column-name)))

(defn quote*
  [s]
  (QueryBuilder/quote (name s)))

(def ^:private query-type-map
  {:in in
   :=  eq
   =   eq
   :>  gt
   >   gt
   :>= gte
   >=  gte
   :<  lt
   <   lt
   :<= lte
   <=  lte})

(defprotocol WhereBuilder
  (build-where [construct query-builder]))

(extend-protocol WhereBuilder
  clojure.lang.IPersistentVector
  (build-where [construct ^Select$Where query-builder]
    (reduce
     (fn [^Select$Where builder [query-type column value]]
       (if-let [eq-type (query-type-map query-type)]
         (.and builder ((query-type-map query-type) (name column) value))
         (throw (IllegalArgumentException. (str query-type " is not a valid Clause")))
         ))
     query-builder
     construct))
  clojure.lang.IPersistentMap
  (build-where [construct ^Select$Where query-builder]
    (reduce
     (fn [^Select$Where builder [column value]]
       (.and builder (eq (name column) value)))
     query-builder
     construct)))

(def ^:private select-order
  {:what      1
   :from      2
   :where     3
   :order     4
   :limit     4
   :filtering 5})


;;
;; Columns
;;

(defn ttl
  [column]
  (fn ttl-query [query-builder]
    (.ttl query-builder column)))

(defn distinct*
  [column]
  (fn distinct-query [query-builder]
    (.distinct (.column query-builder column))))

(defn all
  []
  (fn all-query [query-builder]
    (.all query-builder))
  )
(defn as
  [wrapper alias]
  (fn distinct-query [query-builder]
    (.as (wrapper query-builder) alias)))

(defn columns
  [& columns]
  [:what (fn [^Select$Selection query-builder]
           (reduce (fn [^Select$Selection builder column]
                     (if (string? column)
                       (.column builder column)
                       (column builder)))
                   query-builder
                   columns))])


(defn column
  [column & {:keys [as]}]
  [:what (fn column-query [^Select$Selection query-builder]
           (let [c (.column query-builder column)]
             (if as
               (.as c as)
               c)))])

(defn where
  [m]
  [:where
   (fn where-query [^Select query-builder]
     (build-where m (.where query-builder)))])

(defn order-by
  [& orderings]
  [:order
   (fn order-by-query [^Select$Where query-builder]
     (.orderBy query-builder (into-array orderings)))])

(defn limit
  [lim]
  [:limit
   (fn order-by-query [^Select  query-builder]
     (.limit query-builder lim))])

(defn allow-filtering
  []
  [:filtering
   (fn order-by-query [^Select  query-builder]
     (.allowFiltering query-builder))])

(defn- from
  [^String table-name]
  [:from (fn from-query [^Select$Selection query-builder]
           (.from query-builder (name table-name))
           )])

(defn- complete-select-query
  [statements]
  (let [query-map (into {} statements)]
    (if (nil? (:what query-map))
      (conj query-map
            [:what (all)])
      statements)))

(defn select
  [table-name & statements]
  (->> (conj statements (from (name table-name)))
       (complete-select-query)
       (sort-by #(get select-order (first %)))
       ;; (map println)
       (map second)
       (reduce (fn [builder statement]
                 (println builder statement)
                 (statement builder))
               (QueryBuilder/select)
               )
       (.toString)
       ))

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
;; Object raw(String str)
;; Object fcall(String name, Object... parameters)
;; Object column(String name)
