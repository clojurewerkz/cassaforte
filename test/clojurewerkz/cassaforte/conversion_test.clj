(ns clojurewerkz.cassaforte.conversion-test
  (:use clojure.test
        clojurewerkz.cassaforte.conversion)
  (:require [clojurewerkz.cassaforte.thrift.column-definition :as column-def]
            [clojurewerkz.cassaforte.thrift.column-family-definition :as column-family-def]
            [clojurewerkz.cassaforte.thrift.keyspace-definition :as keyspace-def])
  (:import [org.apache.cassandra.thrift ConsistencyLevel]))


(deftest test-to-consistency-level
  (testing "with keyword values"
    (are [a b] (is (= a (to-consistency-level b)))
         ConsistencyLevel/ANY :any
         ConsistencyLevel/ONE :ONE
         ConsistencyLevel/TWO :TWO
         ConsistencyLevel/THREE :three
         ConsistencyLevel/QUORUM :quorum
         ConsistencyLevel/LOCAL_QUORUM :local_quorum
         ConsistencyLevel/EACH_QUORUM :each_quorum
         ConsistencyLevel/ALL :all))
  (testing "with consistency level enum values"
    (are [a] (is (= a (to-consistency-level a)))
         ConsistencyLevel/ANY
         ConsistencyLevel/ONE
         ConsistencyLevel/TWO
         ConsistencyLevel/THREE
         ConsistencyLevel/QUORUM
         ConsistencyLevel/LOCAL_QUORUM
         ConsistencyLevel/EACH_QUORUM
         ConsistencyLevel/ALL))
  (testing "with string values"
    (are [a b] (is (= a (to-consistency-level b)))
         ConsistencyLevel/ANY "any"
         ConsistencyLevel/ONE "ONE"
         ConsistencyLevel/TWO "TWO"
         ConsistencyLevel/THREE "three"
         ConsistencyLevel/QUORUM "quorum"
         ConsistencyLevel/LOCAL_QUORUM "local_quorum"
         ConsistencyLevel/EACH_QUORUM "each_quorum"
         ConsistencyLevel/ALL "all")))

(deftest test-build-keyspace-definition
  (let [name           "CassaforteTest2"
        strategy-class "org.apache.cassandra.locator.SimpleStrategy"
        strategy-opts  {"replication_factor" "1"}
        cf-defs        [(build-cfd "CassaforteTest2" "column-family-name" [(build-cd "name" "UTF8Type")])]
        ks-def         (build-keyspace-definition name strategy-class cf-defs :strategy-opts strategy-opts)]
    (is (= name (keyspace-def/get-name ks-def)))
    (is (= strategy-class (keyspace-def/get-strategy-class ks-def)))
    (is (= strategy-opts (keyspace-def/get-strategy-options ks-def)))
    (is (= "column-family-name" (-> (keyspace-def/get-cf-defs ks-def)
                                    first
                                    column-family-def/get-name)))
    (is (= "name" (-> (keyspace-def/get-cf-defs ks-def)
                      first
                      column-family-def/get-column-metadata
                      first
                      column-def/get-name)))))

(deftest test-build-column-definition
  (let [name             "column-definition-test"
        validation-class "org.apache.cassandra.db.marshal.BytesType"
        cdef             (build-column-definition name validation-class)]
    (is (= name (column-def/get-name cdef)))
    (is (= validation-class (column-def/get-validation-class cdef)))))

(deftest test-build-column-family-definition
  (let [keyspace           "CassaforteTest2"
        name               "column-family-name"
        column-definitions [(build-cd "birth_date" "LongType") (build-cd "full_name" "UTF8Type")]
        cfdef              (build-column-family-definition keyspace name column-definitions)]
    (is (= keyspace (column-family-def/get-keyspace cfdef)))
    (is (= name (column-family-def/get-name cfdef)))
    ;; Order is guaranteed by Comparator, so that test does have a predictable order
    (is (= "birth_date" (column-def/get-name (first (column-family-def/get-column-metadata cfdef)))))
    (is (= "full_name" (column-def/get-name (second (column-family-def/get-column-metadata cfdef)))))))

(deftest t-build-column
  (let [name      :name
        value     "John Doe"
        timestamp (System/currentTimeMillis)
        column    (build-column clojurewerkz.support.string/to-byte-buffer name value timestamp)
        c-map     (to-map column)]
    (are [expected actual] (is (= expected actual))
         name (:name c-map)
         value (:value c-map)
         timestamp (:timestamp c-map))))

(deftest t-build-super-column
  (let [key          "a0b1c2"
        column-map   {:age "26" :last_name "P" :first_name "Alex"}
        timestamp    (System/currentTimeMillis)
        super-column (build-super-column clojurewerkz.support.string/to-byte-buffer key column-map timestamp)
        sc-map       (to-map super-column)
        columns      (:columns sc-map)]
    (is (= key (:name sc-map)))
    (is (= :age (:name (first columns))))
    (is (= "26" (:value (first columns))))
    (is (= :last_name (:name (second columns))))
    (is (= "P" (:value (second columns))))
    (is (= :first_name (:name (nth columns 2))))
    (is (= "Alex" (:value (nth columns 2))))))
