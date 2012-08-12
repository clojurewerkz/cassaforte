(ns clojurewerkz.cassaforte.thrift.column-or-super-column
  (:import [org.apache.cassandra.thrift ColumnOrSuperColumn]))

(defn get-column
  [^ColumnOrSuperColumn cdef]
  (.getColumn cdef))
