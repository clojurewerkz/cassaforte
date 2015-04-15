(ns clojurewerkz.cassaforte.new-query-api
  "Functions for building dynamic CQL queries, in case you feel
   that `cql` namespace is too limiting for you."
  (:import [com.datastax.driver.core.querybuilder QueryBuilder
            Select$Selection Select Select$Where
            BindMarker
            Clause]
           [com.datastax.driver.core TupleType DataType]
           ))

;;
;; Static QB Methods
;;

(defn ?
  ([]
     (QueryBuilder/bindMarker))
  ([name]
     (QueryBuilder/bindMarker name)))

(defn timestamp
  [column-name]
  (QueryBuilder/timestamp column-name))

(defn token
  [& column-names]
  (QueryBuilder/token (into-array column-names)))

(defn function-call
  [name & args]
  (QueryBuilder/fcall name (object-array args)))


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

(let [eq  (fn [^String column ^Object value]
            (QueryBuilder/eq column value))
      in  (fn [^String column values]
            (QueryBuilder/in column values))

      lt  (fn [^String column ^Object value]
            (QueryBuilder/lt column value))

      gt  (fn [^String column ^Object value]
            (QueryBuilder/gt column value))

      lte (fn [^String column ^Object value]
            (QueryBuilder/lte column value))

      gte (fn [^String column ^Object value]
            (QueryBuilder/gte column value))]

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
    (to-clauses [construct]))

  (extend-protocol WhereBuilder
    clojure.lang.IPersistentVector
    (to-clauses [construct]
      (reduce
       (fn [acc [query-type column value]]
         (if-let [eq-type (query-type-map query-type)]
           (conj acc ((query-type-map query-type) (name column) value))
           (throw (IllegalArgumentException. (str query-type " is not a valid Clause")))
           ))
       []
       construct))
    clojure.lang.IPersistentMap
    (to-clauses [construct]
      (reduce
       (fn [acc [column value]]
         (conj acc (eq (name column) value)))
       []
       construct))))

;;
;; Tuples
;;

(def primitive-types
  {:ascii     (DataType/ascii)
   :bigint    (DataType/bigint)
   :blob      (DataType/blob)
   :boolean   (DataType/cboolean)
   :counter   (DataType/counter)
   :decimal   (DataType/decimal)
   :double    (DataType/cdouble)
   :float     (DataType/cfloat)
   :inet      (DataType/inet)
   :int       (DataType/cint)
   :text      (DataType/text)
   :timestamp (DataType/timestamp)
   :uuid      (DataType/uuid)
   :varchar   (DataType/varchar)
   :varint    (DataType/varint)
   :timeuuid  (DataType/timeuuid)})

(defn list-type
  [primitive-type]
  (DataType/list (get primitive-types primitive-type)))

(defn set-type
  [primitive-type]
  (DataType/set (get primitive-types primitive-type)))

(defn map-type
  [key-type value-type]
  (DataType/map (get primitive-types key-type)
                (get primitive-types value-type)))

(defn tuple-of
  [types values]
  (.newValue (TupleType/of (into-array (map #(get primitive-types %) types)))
             (object-array values)))

;;
;; Columns
;;

(defn write-time
  [column]
  (fn writetime-query [query-builder]
    (.writeTime query-builder (name column))))

(defn ttl-column
  [column]
  (fn ttl-query [query-builder]
    (.ttl query-builder (name column))))

(defn distinct*
  [column]
  (fn distinct-query [query-builder]
    (.distinct (.column query-builder column))))

(defn count-all
  []
  [:what-count nil])

(defn fcall
  [name & args]
  [:what-fcall [name args]])

(defn all
  []
  ;; TODO: resolve API inconsistency
  [:what-all nil])

(defn as
  [wrapper alias]
  (fn distinct-query [query-builder]
    (.as (wrapper query-builder) alias)))

(defn columns
  [& columns]
  [:what-columns columns])


(defn column
  [column & keys]
  [:what-column [column keys]])

(defn where
  [m]
  [:where m])

(defn order-by
  [& orderings]
  [:order orderings])

(defn limit
  [lim]
  [:limit lim])

(defn allow-filtering
  []
  [:filtering nil])

(defn- from
  [^String table-name]
  [:from table-name])

(defn- complete-select-query
  [statements]
  (let [query-map (into {} statements)]
    (if (nil? (or (:what-columns query-map)
                  (:what-column query-map)
                  (:what-count query-map)
                  (:what-fcall query-map)))
      (conj statements (all))
      statements)))

(let [order     {:what-count   1
                 :what-fcall   1
                 :what-columns 1
                 :what-column  1
                 :from         2
                 :where        3
                 :order        4
                 :limit        4
                 :filtering    5}
      renderers
      {:what-count   (fn count-all-query [query-builder _]
                       (.countAll query-builder))
       :what-fcall   (fn fcall-query [query-builder [name args]]
                       (.fcall query-builder name (to-array args)))
       :what-columns (fn [query-builder columns]
                       (reduce (fn [builder column]
                                 (if (string? column)
                                   (.column builder column)
                                   (column builder)))
                               query-builder
                               columns))
       :what-column  (fn column-query [query-builder [column {:keys [as]}]]
                       (let [c (.column query-builder (name column))]
                         (if as
                           (.as c as)
                           c)))
       :where        (fn where-query [query-builder m]
                       (let [query-builder (.where query-builder)]
                         (doseq [clause (to-clauses m)]
                           (.and query-builder clause))
                         query-builder))
       :what-all     (fn all-query [query-builder _]
                       (.all query-builder))
       :order        (fn order-by-query [query-builder orderings]
                       (.orderBy query-builder (into-array orderings)))

       :limit        (fn order-by-query [query-builder lim]
                       (.limit query-builder lim))

       :filtering    (fn order-by-query [query-builder _]
                       (.allowFiltering query-builder))

       :from         (fn from-query [query-builder table-name]
                       (.from query-builder (name table-name)))
       }]
  (defn select
    [table-name & statements]
    (->> (conj statements (from (name table-name)))
         (complete-select-query)
         (sort-by #(get order (first %)))
         ;; (map second)
         (reduce (fn [builder [statement-name statement-args]]
                   ((get renderers statement-name) builder statement-args))
                 (QueryBuilder/select))
         (.toString)
         )))

;;
;; Insert Query
;;

(defn value
  [key value]
  [:value [key value]])

(defn if-not-exists
  []
  [:if-not-exists nil])

(defn values
  ([m]
     [:values-vector m])
  ([key-seq value-seq]
     [:values-seq [key-seq value-seq]]))

(defn using
  [m]
  [:using m])

(def ^:private with-values
  {:timestamp #(QueryBuilder/timestamp %)
   :ttl       #(QueryBuilder/ttl %)})
(let [order       {:values        1
                   :value         1
                   :values-vector 1
                   :values-seq    1
                   :using         2
                   :if-not-exists 3}
      renderers
      {:value         (fn value-query [query-builder [key value]]
                        (.value query-builder (name key) value))
       :if-not-exists (fn value-query [query-builder _]
                        (.ifNotExists query-builder))
       :values-vector (fn values-query [query-builder m]
                        (.values query-builder (into-array (map name (keys m))) (object-array (vals m))))
       :values-seq    (fn order-by-query [query-builder [key-seq value-seq]]
                        (.values query-builder (into-array (map name key-seq)) (object-array value-seq)))
       :using         (fn using-query [query-builder m]
                        (doseq [[key value] m]
                          (.using query-builder ((get with-values key) value)))
                        query-builder)}]
  (defn insert
    [table-name & statements]
    (->> statements
         (sort-by #(get order (first %)))
         (reduce (fn [builder [statement-name statement-args]]
                   ((get renderers statement-name) builder statement-args))
                 (QueryBuilder/insertInto (name table-name)))
         (.toString))))

;;
;; Update Query
;;

(defn- set-column-
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

(defn- make-assignment
  [[k v]]
  (if (fn? v)
    (v (name k))
    (set-column- (name k) v)))

(defn only-if
  [m]
  [:only-if m])

(defn- update-records-statement
  [m]
  [:values m])

(let [order {:where   1
             :values  2
             :only-if 4}
      renderers
      {:only-if (fn only-if-query [query-builder]
                  (let [[first & more] (to-clauses m)
                        query-builder  (.onlyIf query-builder first)]
                    (doseq [current more]
                      (.and query-builder current))
                    query-builder))
       :values  (if (empty? m)
                  (fn update-records-statement-inner [query-builder]
                    query-builder)
                  (fn update-records-statement-inner [query-builder]
                    (let [first-pair    (first m)
                          more-pairs    (rest m)
                          query-builder (.with query-builder (make-assignment first-pair))]
                      (doseq [kvp more-pairs]
                        (.and query-builder (make-assignment kvp)))
                      query-builder)))
       :where   (fn where-query [query-builder m]
                  (let [query-builder (.where query-builder)]
                    (doseq [clause (to-clauses m)]
                      (.and query-builder clause))
                    query-builder))
       }
      ]
  (defn update
    [table-name records & statements]
    (->> (conj statements (update-records-statement records))
         (sort-by #(get insert-order (first %)))
         (map second)
         (reduce (fn [builder statement]
                   (println statement builder)
                   (statement builder))
                 (QueryBuilder/update (name table-name)))
         (.toString))))

;;
;; Delete Query
;;

(comment
  (defn delete
    [table-name & ]
    (->> statements)
    ))


;; Delete.Builder delete(String... columns)
;; Delete.Selection delete()
;; Batch batch(RegularStatement... statements)
;; Batch unloggedBatch(RegularStatement... statements)
;; Truncate truncate(String table)
;; Truncate truncate(String keyspace, String table)
;; Truncate truncate(TableMetadata table)
;; String quote(String columnName)
;; Assignment set(String name, Object value)

;; Object raw(String str)
;;
;;
