(ns clojurewerkz.cassaforte.bytes-test
  (:require [clojurewerkz.cassaforte.client :as cc])
  (:use clojurewerkz.cassaforte.bytes
   clojure.test)
  (:import [clojurewerkz.cassaforte.serializers
            AbstractSerializer IntegerSerializer StringSerializer LongSerializer
            BooleanSerializer BigIntegerSerializer]))

(deftest t-serializer-roundtrip
  (are [type value]
       (= value (deserialize type (encode value)))
       "Int32Type"(Integer. 1)
       "IntegerType" (java.math.BigInteger. "123456789")
       "LongType" (Long. 100)
       "UTF8Type" "some fancy string"
       "BooleanType" true
       "BooleanType" false
)

  (is (= ["a" "b" "c"]
         (map #(.fromBytes (StringSerializer.) %)
              (.fromByteBuffer composite-serializer
                               (.toByteBuffer composite-serializer ["a" "b" "c"]))))))