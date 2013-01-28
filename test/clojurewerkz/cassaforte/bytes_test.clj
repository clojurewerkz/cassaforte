(ns clojurewerkz.cassaforte.bytes-test
  (:require [clojurewerkz.cassaforte.client :as cc])
  (:use clojurewerkz.cassaforte.bytes
   clojure.test)
  (:import [java.nio ByteBuffer]
           [clojurewerkz.cassaforte.serializers
            AbstractSerializer IntegerSerializer StringSerializer LongSerializer
            BooleanSerializer BigIntegerSerializer]))

(defn to-bytes
  [^ByteBuffer byte-buffer]
  (let [bytes (byte-array (.remaining byte-buffer))]
    (.get byte-buffer bytes 0 (count bytes))
    bytes))

(deftest t-serializer-roundtrip
  (are [type value]
       (= value (deserialize type (to-bytes (encode value))))
       "Int32Type"(Integer. 1)
       "IntegerType" (java.math.BigInteger. "123456789")
       "LongType" (Long. 100)
       "UTF8Type" "some fancy string"
       "BooleanType" true
       "BooleanType" false
       "DateType"(java.util.Date.))

  (is (= ["a" "b" "c"]
         (map #(.fromBytes (StringSerializer.) %)
              (.fromByteBuffer composite-serializer
                               (.toByteBuffer composite-serializer ["a" "b" "c"]))))))