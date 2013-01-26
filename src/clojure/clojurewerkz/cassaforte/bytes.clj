(ns clojurewerkz.cassaforte.bytes
  (:import java.nio.ByteBuffer java.util.Date
           org.apache.cassandra.utils.ByteBufferUtil
           [clojurewerkz.cassaforte.serializers
            IntegerSerializer StringSerializer LongSerializer BooleanSerializer BigIntegerSerializer]
           ))


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

(def ^:dynamic serializers
  {java.lang.Integer (IntegerSerializer.)
   java.lang.Long (LongSerializer.)
   java.lang.String (StringSerializer.)
   java.lang.Boolean (BooleanSerializer.)
   java.math.BigInteger (BigIntegerSerializer.)
  }
  )
(defn ^ByteBuffer encode
  [value]
  (let [serializer (get-in serializers [(type value)])]
    (.toByteBuffer serializer value))

  ;; (if (instance? (Class/forName "[B") value)
  ;;   (java.nio.ByteBuffer/wrap value)
  ;;   (ByteBufferUtil/bytes value))



  )