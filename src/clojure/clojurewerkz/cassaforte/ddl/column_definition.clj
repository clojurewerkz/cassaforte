(ns clojurewerkz.cassaforte.ddl.column-definition
  (:use    [clojurewerkz.cassaforte.bytes :only [encode]])
  (:import [org.apache.cassandra.thrift IndexType ColumnDef]))


;;
;; Index Type conversions
;;

(defprotocol IndexTypeConversion
  (to-index-type [input] "Converts the input to IndexType enum value"))
(extend-protocol IndexTypeConversion
  IndexType
  (to-index-type [^IndexType input]
    input)

  String
  (to-index-type [^String input]
    (IndexType/valueOf (.toUpperCase input)))

  clojure.lang.Keyword
  (to-index-type [^clojure.lang.Keyword input]
    (IndexType/valueOf (.toUpperCase (name input)))))

;;
;; Getters
;;

(defn get-name
  [^ColumnDef cdef]
  (String. (.getName cdef)))

(defn get-validation-class
  [^ColumnDef cdef]
  (.getValidation_class cdef))


;;
;; Builders
;;

(defn ^ColumnDef build-column-definition
  [^String name ^String validation-class & {:keys [index-type index-name]}]
  (let [cd (ColumnDef. (encode name) validation-class)]
    (when index-type
      (.setIndex_type cd (to-index-type index-type)))
    (cond
     index-name       (.setIndex_name cd index-name)
     (and index-name
          index-type) (.setIndex_name cd (str name "_" index-type)))
    cd))

(def build-cd build-column-definition)
