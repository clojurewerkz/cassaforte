(ns clojurewerkz.cassaforte.bytes
  (:import java.nio.ByteBuffer))


;;
;; API
;;

(defmulti deserialize
  "Instantiates a CQL value from the given array of bytes"
  (fn [type bytes] type))

(defmethod deserialize "UTF8Type"
  [_ ^bytes bytes]
  (String. bytes "UTF-8"))

(defmethod deserialize "AsciiType"
  [_ ^bytes bytes]
  (String. bytes))

(defmethod deserialize "DoubleType"
  [_ ^bytes bytes]
  (.getDouble (ByteBuffer/wrap bytes)))

(defmethod deserialize "LongType"
  [_ ^bytes bytes]
  (.getLong (ByteBuffer/wrap bytes)))

(defmethod deserialize "UUIDType"
  [_ ^bytes bytes]
  (java.util.UUID/fromString (String. bytes)))

(defmethod deserialize "BooleanType"
  [_ ^bytes bytes]
  (Boolean/valueOf (String. bytes)))
