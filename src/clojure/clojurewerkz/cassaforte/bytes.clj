(ns clojurewerkz.cassaforte.bytes
  "Facility functions to use with serialization, handle deserialization of all the data types
   supported by Cassandra."
  (:import java.nio.ByteBuffer java.util.Date
           org.apache.cassandra.utils.ByteBufferUtil
           [com.datastax.driver.core DataType DataType$Name]
           [org.apache.cassandra.db.marshal UTF8Type Int32Type IntegerType AsciiType FloatType
            DecimalType BytesType DoubleType LongType UUIDType DateType BooleanType ListType
            MapType SetType AbstractType AbstractCompositeType InetAddressType TimeUUIDType
            CounterColumnType]))

(declare serializers)

(defn #^bytes to-bytes
  [^ByteBuffer byte-buffer]
  (let [bytes (byte-array (.remaining byte-buffer))]
    (.get byte-buffer bytes 0 (count bytes))
    bytes))

(def ^:private deserializers
  {DataType$Name/ASCII     (AsciiType/instance)
   DataType$Name/BIGINT    (LongType/instance)
   DataType$Name/BLOB      (BytesType/instance)
   DataType$Name/BOOLEAN   (BooleanType/instance)
   ;; DataType$Name/COUNTER   (CounterColumnType/instance)
   DataType$Name/COUNTER   (LongType/instance)
   DataType$Name/DECIMAL   (DecimalType/instance)
   DataType$Name/DOUBLE    (DoubleType/instance)
   DataType$Name/FLOAT     (FloatType/instance)
   DataType$Name/INET      (InetAddressType/instance)
   DataType$Name/INT       (Int32Type/instance)
   DataType$Name/TEXT      (UTF8Type/instance)
   DataType$Name/TIMESTAMP (DateType/instance)
   DataType$Name/UUID      (UUIDType/instance)
   DataType$Name/VARCHAR   (UTF8Type/instance)
   DataType$Name/VARINT    (IntegerType/instance)
   DataType$Name/TIMEUUID  (TimeUUIDType/instance)})

(defn get-deserializer
  [^DataType t]
  (let [type-name (.getName t)]
    (if-let [deserializer (get deserializers type-name)]
      deserializer
      (cond
       (= type-name (DataType$Name/LIST)) (ListType/getInstance (get-deserializer (-> t (.getTypeArguments) (.get 0))))
       (= type-name (DataType$Name/SET))  (SetType/getInstance (get-deserializer (-> t (.getTypeArguments) (.get 0))))
       (= type-name (DataType$Name/MAP))  (MapType/getInstance (get-deserializer (-> t (.getTypeArguments) (.get 0)))
                                                               (get-deserializer (-> t (.getTypeArguments) (.get 1))))
       :else (throw (Exception. (str "Can't find matching deserializer for: " t)))))))

(defn compose
  [^AbstractType serializer bytes]
  (.compose serializer bytes))

(defn deserialize
  [^DataType dt bytes]
    (compose (get-deserializer dt) bytes)
  )
