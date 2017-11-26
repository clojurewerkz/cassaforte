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

(ns clojurewerkz.cassaforte.metadata
  "Main namespace for getting information about cluster, keyspaces, tables, etc."
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client  :as cc])
  (:import [com.datastax.driver.core Session KeyspaceMetadata Metadata TableMetadata
            ColumnMetadata TableOptionsMetadata Host UserType MaterializedViewMetadata
            FunctionMetadata AggregateMetadata DataType IndexMetadata ClusteringOrder
            AbstractTableMetadata DataType$Name]
           [java.util Collection Map]))

;; Auxiliary functions, maybe need to be re-implemented
(def ^:private not-nil? (comp not nil?))

(defn- non-nil-coll [conv-func coll]
  (filterv not-nil? (mapv conv-func coll)))

(defn- process-into-map
  [conv-func field coll]
  (into {} (non-nil-coll (fn [d] (when-let [m (conv-func d)]
                                   [(get m field) m]))
                         coll)))

(defn- into-keyed-map [^Map coll]
  (zipmap (map keyword (.keySet coll)) (.values coll)))

;; Functions for conversion of different objects into maps
(defn- convert-data-type [^DataType dt]
  (when dt
    (let [type-args (.getTypeArguments dt)
          nm (.getName dt)
          is-udt? (= nm DataType$Name/UDT)]
      (-> {
           :name (if is-udt?
                   (str (.getKeyspace ^UserType dt) "." (.getTypeName ^UserType dt) )
                   (str nm))
           :collection? (.isCollection dt)
           :frozen? (.isFrozen dt)
           :user-defined? is-udt?
           }
          (cond-> (seq type-args) (assoc :type-args (mapv convert-data-type type-args)))))))

(defn- convert-user-types-meta [^UserType ut-meta]
  (when ut-meta
    {
     :name (keyword (.getTypeName ut-meta))
     :frozen? (.isFrozen ut-meta)
     :keyspace (keyword (.getKeyspace ut-meta))
     :fields (into {}
                   (filter not-nil?
                           (mapv (fn [^String field-name]
                                   (when-let [ut (convert-data-type (.getFieldType ut-meta field-name))]
                                     [(keyword field-name) ut]))
                                 (.getFieldNames ut-meta))))
     :cql (.asCQLQuery ut-meta)
     }))

(defn- convert-func-meta [^FunctionMetadata fn-meta]
  (when fn-meta
    (let [args (.getArguments fn-meta)]
      {
       :cql (.asCQLQuery fn-meta)
       :name (.getSignature fn-meta)
       :simple-name (.getSimpleName fn-meta)
       :arguments (into {} (zipmap (map keyword (.keySet args))
                                   (map convert-data-type (.values args))))
       :language (.getLanguage fn-meta)
       :callable-on-nil? (.isCalledOnNullInput fn-meta)
       :body (.getBody fn-meta)
       :return-type (convert-data-type (.getReturnType fn-meta))
       })))

(defn- convert-index-meta [^IndexMetadata idx-meta]
  (when idx-meta
    (let [idx-class (.getIndexClassName idx-meta)]
      (-> {
           :custom? (.isCustomIndex idx-meta)
           :name (keyword (.getName idx-meta))
           :kind (keyword (.name (.getKind idx-meta)))
           :target (.getTarget idx-meta)
           :cql (.asCQLQuery idx-meta)}
          (cond-> idx-class (assoc :index-class idx-class))))))

(defn- convert-aggr-meta [^AggregateMetadata agg-meta]
  (when agg-meta
    {
     :name (.getSignature agg-meta)
     :simple-name (.getSimpleName agg-meta)
     :return-type (convert-data-type (.getReturnType agg-meta))
     :arguments (filterv not-nil? (mapv convert-data-type (.getArgumentTypes agg-meta)))
     :cql (.asCQLQuery agg-meta)
     :state-type (convert-data-type (.getStateType agg-meta))
     :state-func (.. agg-meta getStateFunc getSignature)
     :final-func (.. agg-meta getFinalFunc getSignature)
     :init-state (.getInitCond agg-meta)
     }))

(defn- convert-column [^ColumnMetadata col-meta]
  (when col-meta
    {:static? (.isStatic col-meta)
     :name (keyword (.getName col-meta))
     :type (convert-data-type (.getType col-meta))}))

(defn- convert-table-options [^TableOptionsMetadata opts]
  (when opts
    (let [caching (.getCaching opts)
          compaction (.getCompaction opts)
          comnt (.getComment opts)
          compression (.getCompression opts)
          extensions (.getExtensions opts)]
      (-> {
           :bloom-filter-fp-chance (.getBloomFilterFalsePositiveChance opts)
           :crc-check-chance (.getCrcCheckChance opts)
           :default-ttl (.getDefaultTimeToLive opts)
           :compact-storage? (.isCompactStorage opts)
           :cdc? (.isCDC opts)
           :gc-grace (.getGcGraceInSeconds opts)
           :local-read-repair-chance (.getLocalReadRepairChance opts)
           :read-repair-chance (.getReadRepairChance opts)
           :max-index-interval (.getMaxIndexInterval opts)
           :min-index-interval (.getMinIndexInterval opts)
           :replicate-on-write? (.getReplicateOnWrite opts)
           :speculative-retry (.getSpeculativeRetry opts)
           :memtable-flush-period (.getMemtableFlushPeriodInMs opts)
           :populate-io-cache-on-flush? (.getPopulateIOCacheOnFlush opts)
           }
          (cond-> caching (assoc :caching (into-keyed-map caching)))
          (cond-> (seq comnt) (assoc :comment comnt))
          (cond-> compaction (assoc :compaction (into-keyed-map compaction)))
          (cond-> extensions (assoc :extensions (into-keyed-map extensions)))
          (cond-> compression (assoc :compression (into-keyed-map compression)))))))

;; TODO: Decide - do we need to include primary key, partition key, clustering & regular
;; columns as separate slots?  Or it's better to leave filtering to user?
(defn- convert-abstract-table-meta [^AbstractTableMetadata at-meta]
  (when at-meta
    (let [id (.getId at-meta)
          partition-key-names (non-nil-coll (fn [^ColumnMetadata v] (keyword (.getName v)))
                                            (.getPartitionKey at-meta))
          clustering-names (non-nil-coll (fn [^ColumnMetadata v] (keyword (.getName v)))
                                         (.getClusteringColumns at-meta))
          non-regular (into {} (concat (mapv #(vector % :partition-key) partition-key-names)
                                       (mapv #(vector % :clustering) clustering-names)))
          columns (non-nil-coll convert-column (.getColumns at-meta))
          columns (mapv #(assoc % :kind (get non-regular (:name %) :regular)) columns)
          primary-key (filterv #(not= (:kind %) :regular) columns)
          partition-key (filterv #(= (:kind %) :partition-key) columns)
          clustering-columns (filterv #(= (:kind %) :clustering) columns)
          clustering-order (.getClusteringOrder at-meta)
          options (convert-table-options (.getOptions at-meta))
          regular-columns (filterv #(= (:kind %) :regular) columns)]
      (-> {
           :name (keyword (.getName at-meta))
           :cql (.asCQLQuery at-meta)
           :columns columns
           :primary-key primary-key
           :partition-key partition-key
           }
          (cond-> (seq regular-columns) (assoc :regular-columns regular-columns))
          (cond-> (seq clustering-columns)
            (assoc :clustering-columns
                   (mapv (fn [v1 ^ClusteringOrder v2]
                           (assoc v1 :order (keyword (.name v2))))
                         clustering-columns clustering-order)))
          (cond-> id (assoc :id id))
          (cond-> options (assoc :options options))))))

(defn- convert-mv-meta [^MaterializedViewMetadata mv-meta]
  (when mv-meta
    (assoc (convert-abstract-table-meta mv-meta)
           :base-table (keyword (.. mv-meta getBaseTable getName)))))

;; http://docs.datastax.com/en/drivers/java/3.3/com/datastax/driver/core/TableMetadata.html
(defn- convert-table-meta [^TableMetadata table-meta]
  (when table-meta
    (let [indexes (process-into-map convert-index-meta :name (.getIndexes table-meta))
          mvs (process-into-map convert-mv-meta :name (.getViews table-meta))]
      (-> (convert-abstract-table-meta table-meta)
          (cond-> (seq indexes) (assoc :indexes indexes))
          (cond-> (seq mvs) (assoc :materialized-views mvs))))))

;; http://docs.datastax.com/en/drivers/java/3.3/com/datastax/driver/core/KeyspaceMetadata.html
(defn- convert-keyspace-meta
  "Converts KeyspaceMetadata into Clojure map"
  [^KeyspaceMetadata ks-meta detailed?]
  (when ks-meta 
    (let [replication (.getReplication ks-meta)
          tbl-meta (.getTables ks-meta)
          tables (if detailed?
                   (process-into-map convert-table-meta :name tbl-meta)
                   (mapv (fn [^TableMetadata v]
                           (keyword (.getName v))) (seq tbl-meta)))
          ut-meta (.getUserTypes ks-meta)
          user-types (if detailed?
                       (process-into-map convert-user-types-meta :name ut-meta)
                       (mapv (fn [^UserType v]
                               (keyword (.getTypeName v))) (seq ut-meta)))
          fn-meta (.getFunctions ks-meta)
          functions (if detailed?
                      (process-into-map convert-func-meta :name fn-meta)
                      (mapv (fn [^FunctionMetadata v]
                              (.getSignature v)) (seq fn-meta)))
          aggr-meta (.getAggregates ks-meta)
          aggregates (if detailed?
                       (process-into-map convert-aggr-meta :name aggr-meta)
                       (mapv (fn [^AggregateMetadata v]
                               (.getSignature v)) (seq aggr-meta)))
          mvs-meta (.getMaterializedViews ks-meta)
          materialized-views (if detailed?
                               (process-into-map convert-mv-meta :name mvs-meta)
                               (mapv (fn [^MaterializedViewMetadata v]
                                       (keyword (.getName v))) (seq mvs-meta)))]
      (-> {
           :replication (into-keyed-map replication)
           :name (keyword (.getName ks-meta))
           :durable-writes (.isDurableWrites ks-meta)
           }
          (cond-> (seq tables) (assoc :tables tables))
          (cond-> (seq user-types) (assoc :user-types user-types))
          (cond-> (seq functions) (assoc :functions functions))
          (cond-> (seq aggregates) (assoc :aggregates aggregates))
          (cond-> (seq materialized-views) (assoc :materialized-views materialized-views))
          (cond-> detailed? (assoc :cql (.asCQLQuery ks-meta)))))))

(defn- ^Metadata get-cluster-meta [^Session session]
  (when session
    (when-let [cluster (.getCluster session)]
      (.getMetadata cluster))))

(defn- ^KeyspaceMetadata get-keyspace-meta [^Session session ks]
  (when-let [cluster-meta ^Metadata (get-cluster-meta session)]
    (.getKeyspace cluster-meta (name ks))))

(defn keyspace
  "Describes a keyspace.
   Verbosity is regulated by :detailed? parameter that is equal to false by default"
  [^Session session ks & {:keys [detailed?] :or {detailed? false}}]
  (convert-keyspace-meta (get-keyspace-meta session ks) detailed?))

(defn keyspaces
  "Describes all available keyspaces.
  Verbosity is regulated by :detailed? parameter that is equal to false by default"
  [^Session session & {:keys [detailed?] :or {detailed? false}}]
  (when-let [cluster-meta ^Metadata (get-cluster-meta session)]
    (process-into-map #(convert-keyspace-meta % detailed?)
                      :name (.getKeyspaces cluster-meta))))

(defn table
  "Describes a table"
  [^Session session ks table]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (convert-table-meta (.getTable ks-meta (name table)))))

(defn tables
  "Returns descriptions of all the tables"
  [^Session session ks]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (process-into-map convert-table-meta :name (.getTables ks-meta))))

(defn index
  "Describes an index"
  [^Session session ks table index]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (when-let [tbl (.getTable ks-meta (name table))]
      (convert-index-meta (.getIndex tbl (name index))))))

(defn indexes
  "Returns descriptions of indices"
  [^Session session ks table]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (when-let [tbl (.getTable ks-meta (name table))]
      (process-into-map convert-index-meta :name (.getIndexes tbl)))))

(defn materialized-view
  "Describes a materialized view"
  [^Session session ks mv]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (convert-mv-meta (.getMaterializedView ks-meta (name mv)))))

(defn materialized-views
  "Returns descriptions of all materialized views"
  [^Session session ks]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (process-into-map convert-mv-meta :name (.getMaterializedViews ks-meta))))

(defn user-type
  "Describes a "
  [^Session session ks utype]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (convert-user-types-meta (.getUserType ks-meta (name utype)))))

(defn user-types
  "Returns descriptions of all user types"
  [^Session session ks]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (process-into-map convert-user-types-meta :name (.getUserTypes ks-meta))))

(defn aggregate
  "Describes a "
  [^Session session ks aggr ^Collection arg-types]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (convert-aggr-meta (.getAggregate ks-meta (name aggr) arg-types))))

(defn aggregates
  "Returns descriptions of all aggregates"
  [^Session session ks]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (process-into-map convert-aggr-meta :name (.getAggregates ks-meta))))

(defn function
  "Describes a "
  [^Session session ks func ^Collection arg-types]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (convert-func-meta (.getFunction ks-meta (name func) arg-types))))

(defn functions
  "Returns descriptions of all functions"
  [^Session session ks]
  (when-let [ks-meta (get-keyspace-meta session ks)]
    (process-into-map convert-func-meta :name (.getFunctions ks-meta))))

(defn columns
  "Describes columns of a table"
  [^Session session ks tbl-name]
  (:columns (table session ks tbl-name)))

(defn- get-host-info [^Host host detailed?]
  (when host
    (-> {:rack (.getRack host)
         :datacenter (.getDatacenter host)
         :up? (.isUp host)
         :state (.getState host)
         :cassandra-version (str (.getCassandraVersion host))
         ;;     :dse-version (str (.getDseVersion host))
         ;;     :dse-workloads (.getDseWorkloads host)
         :socket-address (str (.getSocketAddress host))
         :listen-address (str (.getListenAddress host))
         :address (str (.getAddress host))
         :broadcast-address (str (.getBroadcastAddress host))
         }
        (cond-> detailed? (assoc :tokens (.getTokens host))))))

(defn- get-hosts-impl [^Metadata cluster-meta detailed?]
  (when-let [hosts (.getAllHosts cluster-meta)]
    (non-nil-coll #(get-host-info % detailed?) hosts)))

(defn cluster
  "Describes cluster.
  Verbosity is regulated by :detailed? parameter that is equal to false by default"
  [^Session session & {:keys [detailed?] :or {detailed? false}}]
  (when-let [cluster-meta ^Metadata (get-cluster-meta session)]
    {
     :name        (keyword (.getClusterName cluster-meta))
     :hosts       (get-hosts-impl cluster-meta detailed?)
     :partitioner (.getPartitioner cluster-meta)
     :keyspaces   (process-into-map #(convert-keyspace-meta % detailed?)
                                    :name (.getKeyspaces cluster-meta))
     }
    ))


(comment
  (def conn (cc/connect ["127.0.0.1"] {:protocol-version 3}))

  (def conn (cc/connect ["192.168.0.10"] {:protocol-version 3}))
  )
