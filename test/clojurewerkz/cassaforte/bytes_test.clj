(ns clojurewerkz.cassaforte.bytes-test
  (:require [clojurewerkz.cassaforte.client :as cc])
  (:use clojurewerkz.cassaforte.bytes
   clojure.test)
  (:import [clojurewerkz.cassaforte.serializers
            AbstractSerializer IntegerSerializer StringSerializer LongSerializer
            BooleanSerializer BigIntegerSerializer]))

(deftest t-serializer-roundtrip
  (are [serializer value]
       (= value (.fromByteBuffer serializer (.toByteBuffer serializer value)))
       (IntegerSerializer.) (Integer. 1)
       (IntegerSerializer.) (Integer. 100)
       (LongSerializer.) (Long. 100)
       (BigIntegerSerializer.) (java.math.BigInteger. "123456789")
       (StringSerializer.) "somer fancy string"
       (BooleanSerializer.) true
       (BooleanSerializer.) false)

  (is (= ["a" "b" "c"]
         (map #(.fromBytes (StringSerializer.) %)
              (.fromByteBuffer composite-serializer
                               (.toByteBuffer composite-serializer ["a" "b" "c"]))))))