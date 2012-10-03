(ns clojurewerkz.cassaforte.thrift.column-or-super-column
  (:import [org.apache.cassandra.thrift Column SuperColumn ColumnOrSuperColumn]))

;;
;; Getters
;;

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

;;
;; Builders
;;

(defprotocol CoscConversion
  (^ColumnOrSuperColumn build-cosc [input] "Converts given instance to ColumnOrSupercolumn"))

(extend-protocol CoscConversion
  Column
  (build-cosc [^Column input]
    (.setColumn (ColumnOrSuperColumn.) input))
  SuperColumn
  (build-cosc [^SuperColumn input]
    (.setSuper_column (ColumnOrSuperColumn.) input)))
