(ns clojurewerkz.cassaforte.ddl.column-family-definition
  (:import [org.apache.cassandra.thrift CfDef]
           java.util.List))

;;
;; Getters
;;
(defn get-keyspace
  [^CfDef cfdef]
  (.getKeyspace cfdef))

(defn get-name
  [^CfDef cfdef]
  (.getName cfdef))

(defn get-column-type
  [^CfDef cfdef]
  (.getColumn_type cfdef))

(defn get-comparator-type
  [^CfDef cfdef]
  (.getComparator_type cfdef))

(defn get-column-metadata
  [^CfDef cfdef]
  (.getColumn_metadata cfdef))

(def get-cdefs get-column-metadata)

;; get-comment
;; get-read-repair-chance
;; get-default-validation-class
;; get-id

(defn set-keyspace
  [^CfDef cfdef ^String keyspace]
  (.setKeyspace cfdef keyspace))

;;
;; Builders
;;

(defn ^org.apache.cassandra.thrift.CfDef build-column-family-definition
  ([^String name]
     (let [cfdef (CfDef.)]
       (.setName cfdef name)))
  ([^String name ^List cdefs & {:keys [column-type comparator-type keyspace]
                                :or {column-type "Standard"
                                     comparator-type "BytesType"}}]
     (let [^CfDef cfdef (build-column-family-definition name)]
       (when keyspace
         (set-keyspace cfdef keyspace))
       (.setColumn_type cfdef column-type)
       (.setComparator_type cfdef comparator-type)
       (doseq [cd cdefs]
         (.addToColumn_metadata cfdef cd))
       cfdef)))

(def ^org.apache.cassandra.thrift.CfDef build-cfd build-column-family-definition)