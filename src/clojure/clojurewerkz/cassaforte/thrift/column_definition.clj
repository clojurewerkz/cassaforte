(ns clojurewerkz.cassaforte.thrift.column-definition
  (:import [org.apache.cassandra.thrift ColumnDef]))

(defn get-name
  [^ColumnDef cdef]
  (String. (.getName cdef)))

(defn get-validation-class
  [^ColumnDef cdef]
  (.getValidation_class cdef))

;; get-index-type