(ns clojurewerkz.cassaforte.bytes-test
  (:use clojurewerkz.cassaforte.bytes
        clojure.test)
  (:import [java.nio ByteBuffer]))

(deftest t-serializer-roundtrip
  (are [type value]
       (= value (deserialize type (to-bytes (encode value))))
       "Int32Type"(Integer. 1)
       "IntegerType" (java.math.BigInteger. "123456789")
       "LongType" (Long. 100)
       "UTF8Type" "some fancy string"
       "AsciiType" "some fancy string"
       "BooleanType" false
       "BooleanType" true
       "org.apache.cassandra.db.marshal.DateType" (java.util.Date.)
       "DoubleType" (java.lang.Double. "123")
       "ListType(UTF8Type)" ["a" "b" "c"]
       "CompositeType(UTF8Type,UTF8Type,UTF8Type)" (composite "a" "b" "c")))
