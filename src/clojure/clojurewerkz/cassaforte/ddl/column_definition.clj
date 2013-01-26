(ns clojurewerkz.cassaforte.ddl.column-definition
  (:use    [clojurewerkz.cassaforte.bytes :only [encode]])
  (:import [org.apache.cassandra.thrift ColumnDef]))

;;
;; Getters
;;

(defn get-name
  [^ColumnDef cdef]
  (String. (.getName cdef)))

(defn get-validation-class
  [^ColumnDef cdef]
  (.getValidation_class cdef))

;; get-index-type


;;
;; Builders
;;

(defn ^ColumnDef build-column-definition
  [^String name ^String validation-class]
  (ColumnDef. (encode name) validation-class))

(def build-cd build-column-definition)