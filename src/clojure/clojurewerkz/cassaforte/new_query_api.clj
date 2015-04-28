(ns clojurewerkz.cassaforte.new-query-api
  "Functions for building dynamic CQL queries, in case you feel
   that `cql` namespace is too limiting for you."
  (:import [com.datastax.driver.core.querybuilder QueryBuilder
            Select$Selection Select Select$Where
            BindMarker
            Clause]
           [com.datastax.driver.core RegularStatement]
           [com.datastax.driver.core.schemabuilder SchemaBuilder SchemaBuilder$Direction

            SchemaBuilder$Caching SchemaBuilder$KeyCaching]
           )
  (:require [clojurewerkz.cassaforte.aliases :as alias]
            clojurewerkz.cassaforte.query.query-builder
            clojurewerkz.cassaforte.query.types))

(set! *warn-on-reflection* false)

(def ^:dynamic *batch* false)

(defn maybe-stringify
  [statement]
  (if *batch*
    statement
    (.toString statement)))

(alias/alias-ns 'clojurewerkz.cassaforte.query.query-builder)
(alias/alias-ns 'clojurewerkz.cassaforte.query.types)

;;
;; WHERE Statement
;;

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
;; SELECT Statement
;;


;; Columns

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

(defn as
  [wrapper alias]
  (fn distinct-query [query-builder]
    (.as (wrapper query-builder) alias)))

;; Query Parts

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

(defn from
  [table-name]
  [:from (name table-name)])

;; Query

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
    (->> (conj statements (from table-name))
         (complete-select-query)
         (sort-by #(get order (first %)))
         ;; (map second)
         (reduce (fn [builder [statement-name statement-args]]
                   ((get renderers statement-name) builder statement-args))
                 (QueryBuilder/select))
         (maybe-stringify)
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
         (maybe-stringify))))

;;
;; Update Query
;;

(defn- make-assignment
  [[k v]]
  (if (fn? v)
    (v (name k))
    (set-column (name k) v)))

(defn only-if
  [m]
  [:only-if m])

(defn- update-records-statement
  [m]
  (if (empty? m)
    [:empty-values m]
    [:values m]))

(let [order {:where   1
             :values  2
             :only-if 4}
      renderers
      {:only-if      (fn only-if-query [query-builder m]
                       (let [[first & more] (to-clauses m)
                             query-builder  (.onlyIf query-builder first)]
                         (doseq [current more]
                           (.and query-builder current))
                         query-builder))
       :using        (fn using-query [query-builder m]
                       (doseq [[key value] m]
                         (.using query-builder ((get with-values key) value)))
                       query-builder)
       :empty-values (fn update-records-statement-inner [query-builder _]
                       query-builder)
       :values       (fn update-records-statement-inner [query-builder m]
                       (let [first-pair    (first m)
                             more-pairs    (rest m)
                             query-builder (.with query-builder (make-assignment first-pair))]
                         (doseq [kvp more-pairs]
                           (.and query-builder (make-assignment kvp)))
                         query-builder))
       :where        (fn where-query [query-builder m]
                       (let [query-builder (.where query-builder)]
                         (doseq [clause (to-clauses m)]
                           (.and query-builder clause))
                         query-builder))}]
  (defn update
    [table-name records & statements]
    (->> (conj statements (update-records-statement records))
         (sort-by #(get order (first %)))
         (reduce (fn [builder [statement-name statement-args]]
                   ((get renderers statement-name) builder statement-args))
                 (QueryBuilder/update (name table-name)))
         (maybe-stringify))))

;;
;; Delete Query
;;

;;
;; Delete column types
;;

(defn if-exists
  []
  [:if-exists nil])

(defn list-elt
  [column-name n]
  (fn list-elt-statement [query-builder]
    (.listElt query-builder (name column-name) n)))

(defn map-elt
  [column-name key]
  (fn map-elt-statement [query-builder]
    (.mapElt query-builder (name column-name) key)))

(let [order {:what-columns 1
             :from         2
             :where        3
             :if-exists    4
             :only-if      5
             :using        6
             }
      renderers
      {:where        (fn where-query [query-builder m]
                       (let [[first-clause & more-clauses] (to-clauses m)
                             query-builder                  (.where query-builder first-clause)]
                         (doseq [clause more-clauses]
                           (.and query-builder clause))
                         query-builder))

       :from         (fn from-query [query-builder table-name]
                       (.from query-builder (name table-name)))

       :what-columns (fn [query-builder columns]
                       (reduce (fn [builder column]
                                 (if (or (keyword? column) (string? column)) ;; Most likely same bug above
                                   (.column builder (name column))
                                   (column builder)))
                               query-builder
                               columns))

       :only-if      (fn only-if-query [query-builder m]
                       (let [[first & more] (to-clauses m)
                             query-builder  (.onlyIf query-builder first)]
                         (doseq [current more]
                           (.and query-builder current))
                         query-builder))

       :if-exists    (fn value-query [query-builder _]
                       (.ifExists query-builder))

       :using        (fn using-query [query-builder m]
                       (doseq [[key value] m]
                         (.using query-builder ((get with-values key) value)))
                       query-builder)
       }]

  (defn delete
    [table-name & statements]
    (->> (conj statements (from (name table-name)))
         (sort-by #(get order (first %)))
         (reduce (fn [builder [statement-name statement-args]]
                   ((get renderers statement-name) builder statement-args))
                 (QueryBuilder/delete))
         (maybe-stringify))))

;;
;; Truncate
;;

(defn truncate
  ([table-name]
     (.toString (QueryBuilder/truncate (name table-name))))
  ([table-name keyspace]
     (.toString (QueryBuilder/truncate (name keyspace) (name table-name)))))

(defmacro queries
  [& statements]
  [:queries `(binding [*batch* true]
               ~(vec statements))])

(let [order
      {:queries 1
       :using   2}
      renderers
      {:queries (fn queries-renderer [query-builder queries]
                  (reduce (fn [builder query]
                            (.add builder query))
                          query-builder
                          queries))
       :using   (fn using-query [query-builder m]
                  (doseq [[key value] m]
                    (.using query-builder ((get with-values key) value)))
                  query-builder)}]
  (defn- generic-batch
    [batch-type statements]
    (->> statements
         (sort-by #(get order (first %)))
         (reduce (fn [builder [statement-name statement-args]]
                   ((get renderers statement-name) builder statement-args))
                 batch-type)
         (maybe-stringify)))

  (defn batch
    [& statements]
    (generic-batch (QueryBuilder/batch
                    (make-array RegularStatement 0))
                   statements))

  (defn unlogged-batch
    [& statements]
    (generic-batch (QueryBuilder/unloggedBatch
                    (make-array RegularStatement 0))
                   statements)))

;;
;; Schema Builder
;;

(defn column-definitions
  [m]
  [:column-definitions m])

(defn- make-column-type
  [column-name column-defs]
  (let [column-type (get column-defs column-name)]
    (if (keyword? column-type)
      (get primitive-types column-type)
      column-type)))

(let [order
      {:column-definitions 1
       :if-not-exists      2}
      renderers
      {:column-definitions (fn [query-builder column-defs]
                             (let [[primary-key & clustering-keys] (get column-defs :primary-key)
                                   ]

                               (reduce
                                (fn [builder [column-name column-type]]
                                  (.addColumn builder
                                              (name column-name)
                                              (make-column-type column-name column-defs)))
                                query-builder
                                (apply dissoc column-defs (conj (flatten (get column-defs :primary-key))
                                                                :primary-key)))

                               (reduce
                                (fn [builder column-name]
                                  (.addPartitionKey builder
                                                    (name column-name)
                                                    (make-column-type column-name column-defs)))
                                query-builder
                                (if (sequential? primary-key)
                                  primary-key
                                  (list primary-key)))

                               (reduce
                                (fn [builder column-name]
                                  (.addClusteringColumn builder
                                                        (name column-name)
                                                        (make-column-type column-name column-defs)))
                                query-builder
                                clustering-keys)))

       :if-not-exists      (fn if-not-exists-query [query-builder _]
                             (.ifNotExists query-builder))}]
  (defn create-table
    [table-name & statements]
    (->> statements
         (sort-by #(get order (first %)))
         (reduce (fn [builder [statement-name statement-args]]
                   ((get renderers statement-name) builder statement-args))
                 (SchemaBuilder/createTable (name table-name)))
         (maybe-stringify))))

;;
;; Alter table
;;

(defn with-options
  [options]
  [:with-options options])

(defn add-column
  [column-name column-type]
  [:add-column [column-name column-type]])

(defn drop-column
  [column-name]
  [:drop-column column-name])

(defn alter-column
  [column-name column-type]
  [:alter-column [column-name column-type]])

(def ^:private alter-options
  {:default-ttl                 (fn [opts val] (.defaultTimeToLive opts val))
   :bloom-filter-fp-chance      (fn [opts val] (.bloomFilterFPChance opts val))
   :caching                     (fn [opts val] (.caching opts val))
   :gc-grace-seconds            (fn [opts val] (.gcGraceSeconds opts val))
   :min-index-interval          (fn [opts val] (.minIndexInterval opts val))
   :index-interval              (fn [opts val] (.indexInterval opts val))
   :max-index-interval          (fn [opts val] (.maxIndexInterval opts val))
   :comment                     (fn [opts val] (.comment opts val))
   :read-repair-chance          (fn [opts val] (.readRepairChance opts val))
   :speculative-retry           (fn [opts val] (.speculativeRetry opts val))
   :dc-local-read-repair-chance (fn [opts val] (.dcLocalReadRepairChance opts val))
   :memtable-flush-period-in-ms (fn [opts val] (.memtableFlushPeriodInMillis opts val))
   :compaction-options          (fn [opts val] (.compactionOptions opts val))
   :compression-options         (fn [opts val] (.compressionOptions opts val))})

(defn resolve-alter-option
  [option-name]
  (if-let [res (get alter-options option-name)]
    res
    (throw (IllegalArgumentException. (str "Alter option "
                                           " was not found, pick one of ("
                                           (clojure.string/join "," (keys alter-options))
                                           ")")))))
(let [order
      {:with-options 1
       :add-column   2
       :alter-column 3
       :drop-column  4}
      renderers
      {:with-options (fn with-options-statement [query-builder options]
                       (reduce
                        (fn [query-builder [option-name option-vals]]
                          ((resolve-alter-option option-name) option-vals))
                        (.withOptions query-builder)
                        options)
                       query-builder)


       :add-column   (fn add-column-statement [query-builder [column-name column-type]]
                       (println column-name 123 column-type)
                       (-> query-builder
                           (.addColumn (name column-name))
                           (.type (resolve-primitive-type column-type))))

       :alter-column (fn alter-column-statement [query-builder [column-name column-type]]
                       (-> query-builder
                           (.alterColumn (name column-name))
                           (.type (resolve-primitive-type column-type))))

       :drop-column  (fn drop-column-statement [query-builder column-name]
                       (.dropColumn query-builder (name column-name)))



       }]

  (defn alter-table
    [table-name & statements]
    (->> statements
         (sort-by #(get order (first %)))
         (reduce (fn [builder [statement-name statement-args]]
                   ((get renderers statement-name) builder statement-args))
                 (SchemaBuilder/alterTable (name table-name)))
         (maybe-stringify)))

  )




;; Alter alterTable(String tableName)
;; Alter alterTable(String keyspaceName, String tableName)
;; Drop dropTable(String tableName)
;; Drop dropTable(String keyspaceName, String tableName)
;; CreateIndex createIndex(String indexName)
;; Drop dropIndex(String indexName)
;; Drop dropIndex(String keyspaceName, String indexName)
;; CreateType createType(String typeName)
;; CreateType createType(String keyspaceName, String typeName)
;; Drop dropType(String typeName)
;; Drop dropType(String keyspaceName, String typeName)
;; UDTType frozen(String udtName)
;; UDTType udtLiteral(String literal)
;; TableOptions.CompactionOptions.SizeTieredCompactionStrategyOptions sizedTieredStategy()
;; TableOptions.CompactionOptions.LeveledCompactionStrategyOptions leveledStrategy()
;; TableOptions.CompactionOptions.DateTieredCompactionStrategyOptions dateTieredStrategy()
;; TableOptions.CompressionOptions noCompression()
;; TableOptions.CompressionOptions lz4()
;; TableOptions.CompressionOptions snappy()
;; TableOptions.CompressionOptions deflate()
;; TableOptions.SpeculativeRetryValue noSpeculativeRetry()
;; TableOptions.SpeculativeRetryValue always()
;; TableOptions.SpeculativeRetryValue percentile(int percentile)
;; TableOptions.SpeculativeRetryValue millisecs(int millisecs)
;; TableOptions.CachingRowsPerPartition noRows()
;; TableOptions.CachingRowsPerPartition allRows()
;; TableOptions.CachingRowsPerPartition rows(int rowNumber)
