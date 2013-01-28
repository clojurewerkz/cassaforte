(ns clojurewerkz.cassaforte.bytes-test
  (:require [clojurewerkz.cassaforte.client :as cc])
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
       "BooleanType" true
       "BooleanType" false
       "DateType" (java.util.Date.)
       "DoubleType" (java.lang.Double. "123"))

  (let [cs (org.apache.cassandra.db.marshal.CompositeType/getInstance
            [org.apache.cassandra.db.marshal.UTF8Type/instance
             org.apache.cassandra.db.marshal.UTF8Type/instance
             org.apache.cassandra.db.marshal.UTF8Type/instance])
        serialized (.decompose cs (to-array ["a" "b" "c"]))]
    (is (= ["a" "b" "c"])
     (map
      #(deserialize "UTF8Type" %)
      (.fromByteBuffer composite-serializer serialized))))

  (let [cs (org.apache.cassandra.db.marshal.ListType/getInstance
            org.apache.cassandra.db.marshal.UTF8Type/instance)
        serialized (.decompose cs ["a" "b" "c"])]
    (is (= ["a" "b" "c"]) (.compose cs serialized))))