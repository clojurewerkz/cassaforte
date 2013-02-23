(ns clojurewerkz.cassaforte.bytes
  (:import java.nio.ByteBuffer java.util.Date
           org.apache.cassandra.utils.ByteBufferUtil
           [clojurewerkz.cassaforte SerializerHelper]
           [org.apache.cassandra.db.marshal UTF8Type Int32Type IntegerType AsciiType FloatType
            DecimalType BytesType DoubleType LongType UUIDType DateType BooleanType CompositeType
            ListType MapType]))

(declare encode)
(declare deserialize)
(declare serializers)

(defn to-bytes
  [^ByteBuffer byte-buffer]
  (let [bytes (byte-array (.remaining byte-buffer))]
    (.get byte-buffer bytes 0 (count bytes))
    bytes))

(defn extract-component
  [bb i]
  (SerializerHelper/extractComponent bb (int i)))

(def ^:dynamic serializers
  {java.nio.HeapByteBuffer (BytesType/instance)
   java.lang.Integer (Int32Type/instance)
   java.math.BigDecimal (DecimalType/instance)
   java.lang.Long (LongType/instance)
   java.lang.Float (FloatType/instance)
   java.lang.Double (DoubleType/instance)
   java.lang.String (UTF8Type/instance)
   java.lang.Boolean (BooleanType/instance)
   java.math.BigInteger (IntegerType/instance)
   java.util.Date (DateType/instance)
   })

;;
;; Clojure Data Type -> ByteBuffer
;;

(defn get-serializer
  [value]
  (cond
   (not (nil? (get serializers (type value)))) (get serializers (type value))
   (= (type value) clojure.lang.PersistentVector) (ListType/getInstance (get-serializer (first value)))
   (map? value) (MapType/getInstance (get-serializer (first (first value)))
                                     (get-serializer (last (first value))))
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

(defn infer-type
  [s]
  (org.apache.cassandra.db.marshal.TypeParser/parse s))

(defn deserialize
  [type-str bytes]
  (let [t (infer-type type-str)]
    (cond
     (isa? MapType (type t)) (into {} (compose t bytes))
     (isa? CompositeType (type t)) (apply composite
                                          (map (fn [i]
                                                 (compose (.get (.types t) i) (to-bytes (extract-component (ByteBuffer/wrap bytes) i))))
                                               (range 0 (count (.types t)))))
     :else (compose t bytes))))


(defn deserialize2
  [t bb]
  (let [bytes (to-bytes bb)]
    (cond
     (isa? MapType (type t)) (into {} (compose t bytes))
     (isa? CompositeType (type t)) (apply composite
                                          (map (fn [i]
                                                 (compose (.get (.types t) i) (to-bytes (extract-component (ByteBuffer/wrap bytes) i))))
                                               (range 0 (count (.types t)))))
     :else (compose t bytes))))
