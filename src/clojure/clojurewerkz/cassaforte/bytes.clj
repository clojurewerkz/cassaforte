(ns clojurewerkz.cassaforte.bytes
  (:import java.nio.ByteBuffer java.util.Date
           org.apache.cassandra.utils.ByteBufferUtil
           [clojurewerkz.cassaforte.serializers AbstractSerializer]
           [org.apache.cassandra.db.marshal UTF8Type Int32Type IntegerType AsciiType
            BytesType
            DoubleType LongType UUIDType DateType BooleanType CompositeType ListType]))

(declare encode)
(declare deserialize)
(declare serializers)

(defn to-bytes
  [^ByteBuffer byte-buffer]
  (let [bytes (byte-array (.remaining byte-buffer))]
    (.get byte-buffer bytes 0 (count bytes))
    bytes))

(def composite-delimiter (byte 0))

(defn- encode-composite-segment
  [raw-value]
  (let [segment ^ByteBuffer (encode raw-value)
        result (ByteBuffer/allocate (+ 3 (.capacity segment)))
        segment-length (short (.capacity segment))]
    (.putShort result segment-length)
    (.put result segment)
    (.put result ^byte composite-delimiter)
    (.rewind result)))

(def composite-serializer
  (proxy [AbstractSerializer] []
    (toByteBuffer [values]
      (let [values-bytes (map (fn [v]
                                (encode-composite-segment v))
                              values)
            capacity (reduce (fn [acc v]
                               (+ acc (.capacity ^ByteBuffer v)))
                             0
                             values-bytes)
            result-buffer (ByteBuffer/allocate capacity)]
        (doseq [v values-bytes]
          (.put result-buffer ^ByteBuffer v))
        (.rewind result-buffer)))

    (fromByteBuffer [byte-buffer]
      (loop [res []]
        (if (> (.remaining byte-buffer) 0)
          (let [segment-length (.getShort byte-buffer)
                segment (byte-array segment-length)]
            (.get byte-buffer segment)
            (.position byte-buffer (inc (.position byte-buffer)))
            (recur (conj res segment)))
          res)))))

(def date-serializer
  (proxy [AbstractSerializer] []
    (toByteBuffer [value]
      (encode (.getTime value)))
    (fromByteBuffer [byte-buffer]
      (Date. (.fromByteBuffer (get serializers java.lang.Long) byte-buffer)))))

(def double-serializer
  (proxy [AbstractSerializer] []
    (toByteBuffer [value]
      (.toByteBuffer (get serializers java.lang.Long)
                     (java.lang.Double/doubleToRawLongBits value)))
    (fromByteBuffer [byte-buffer]
      (let [l (.fromByteBuffer (get serializers java.lang.Long) byte-buffer)]
        (when (not (nil? l))
          (java.lang.Double/longBitsToDouble l))))))

(def ^:dynamic serializers
  {java.nio.HeapByteBuffer (BytesType/instance)
   java.lang.Integer (Int32Type/instance)
   java.lang.Long (LongType/instance)
   java.lang.Double (DoubleType/instance)
   java.lang.String (UTF8Type/instance)
   java.lang.Boolean (BooleanType/instance)
   java.math.BigInteger (IntegerType/instance)
   java.util.Date (DateType/instance)
   [java.lang.String "AsciiType"] (AsciiType/instance)
   })

;;
;; Clojure Data Type -> ByteBuffer
;;

(defn get-serializer
  [value]
  (cond
   (not (nil? (get serializers (type value)))) (get serializers (type value))
   (= (type value) clojure.lang.PersistentVector) (ListType/getInstance (get-serializer (first value)))
   (:composite (meta value)) (CompositeType/getInstance
                              (map get-serializer value))

   :else (throw (Exception. "Can't find matching serializer"))))

(defn ^ByteBuffer encode
  [value]
  (let [serializer (get-serializer value)]
    (if (= CompositeType (type serializer))
      (.decompose serializer (to-array value))
      (.decompose serializer value))))

(defn compose
  [serializer bytes]
  (.compose serializer (ByteBuffer/wrap bytes)))

(defn composite
  [& values]
  (vary-meta values assoc :composite true))

;;
;; ByteBuffer -> Clojure Data Type
;;

(defmulti deserialize
  "Instantiates a CQL value from the given array of bytes"
  (fn [type bytes] (last (clojure.string/split type #"\."))))

(defmethod deserialize "Int32Type"
  [_ ^bytes bytes]
  (compose Int32Type/instance bytes))

(defmethod deserialize "IntegerType"
  [_ ^bytes bytes]
  (compose IntegerType/instance bytes))

(defmethod deserialize "UTF8Type"
  [_ ^bytes bytes]
  (compose UTF8Type/instance bytes))

(defmethod deserialize "AsciiType"
  [_ ^bytes bytes]
  (compose AsciiType/instance bytes))

(defmethod deserialize "BytesType"
  [_ ^bytes bytes]
  bytes)

(defmethod deserialize "DoubleType"
  [_ ^bytes bytes]
  (compose DoubleType/instance bytes))

(defmethod deserialize "LongType"
  [_ ^bytes bytes]
  (compose LongType/instance bytes))

(defmethod deserialize "UUIDType"
  [_ ^bytes bytes]
  (compose UUIDType/instance bytes))

(defmethod deserialize "DateType"
  [_ ^bytes bytes]
  (compose DateType/instance bytes))

(defmethod deserialize "BooleanType"
  [_ ^bytes bytes]
  (compose BooleanType/instance bytes))
