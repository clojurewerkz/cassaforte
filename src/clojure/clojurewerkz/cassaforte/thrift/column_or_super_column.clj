(ns clojurewerkz.cassaforte.thrift.column-or-super-column
  (:import [org.apache.cassandra.thrift ColumnOrSuperColumn]))

(defn get-column
  [^ColumnOrSuperColumn cosc-def]
  (.getColumn cosc-def))

(defn get-super-column
  [^ColumnOrSuperColumn cosc-def]
  (.getSuper_column cosc-def))

(defn is-column?
  [^ColumnOrSuperColumn cosc-def]
  (.isSet_column cosc-def))

(defn is-super-column?
  [^ColumnOrSuperColumn cosc-def]
  (.isSetSuper_column cosc-def))