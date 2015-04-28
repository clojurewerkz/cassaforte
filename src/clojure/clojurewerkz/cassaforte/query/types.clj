(ns clojurewerkz.cassaforte.query.types
  (:import [com.datastax.driver.core TupleType DataType]))

;;
;; Types
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

(defn resolve-primitive-type
  [type-name]
  (if-let [res (get primitive-types type-name)]
    res
    (throw (IllegalArgumentException. (str "Column name "
                                           " was not found, pick one of ("
                                           (clojure.string/join "," (keys primitive-types))
                                           ")")))))

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
