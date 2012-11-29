(ns clojurewerkz.cassaforte.bytes
  (:import java.nio.ByteBuffer java.util.Date
           org.apache.cassandra.utils.ByteBufferUtil))


;;
;; ByteBuffer -> Clojure Data Type
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

(defmethod deserialize "BytesType"
  [_ ^bytes bytes]
  bytes)

(defmethod deserialize "DoubleType"
  [_ ^bytes bytes]
  (.getDouble (ByteBuffer/wrap bytes)))

(defmethod deserialize "LongType"
  [_ ^bytes bytes]
  (.getLong (ByteBuffer/wrap bytes)))

(defmethod deserialize "UUIDType"
  [_ ^bytes bytes]
  (java.util.UUID/fromString (String. bytes)))

(defmethod deserialize "DateType"
  [_ ^bytes bytes]
  (Date. (deserialize "LongType" bytes)))

(defmethod deserialize "BooleanType"
  [_ ^bytes bytes]
  (Boolean/valueOf (String. bytes)))

;;
;; Clojure Data Type -> ByteBuffer
;;

(defn ^ByteBuffer encode
  [value]
  (if (instance? (Class/forName "[B") value)
    (java.nio.ByteBuffer/wrap value)
    (ByteBufferUtil/bytes value)))