(ns clojurewerkz.cassaforte.conversion-test
  (:use clojure.test
        clojurewerkz.cassaforte.conversion
        clojurewerkz.cassaforte.bytes)
  (:require [clojurewerkz.cassaforte.ddl.column :as c]
            [clojurewerkz.cassaforte.ddl.super-column :as sc]
            [clojurewerkz.cassaforte.ddl.column-definition :as cd]
            [clojurewerkz.cassaforte.ddl.column-family-definition :as cfd]
            [clojurewerkz.cassaforte.ddl.keyspace-definition :as kd])
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
        cf-defs        [(cfd/build-cfd "CassaforteTest2" "column-family-name" [(cd/build-cd "name" "UTF8Type")])]
        ks-def         (kd/build-kd name strategy-class cf-defs :strategy-opts strategy-opts)]
    (is (= name (kd/get-name ks-def)))
    (is (= strategy-class (kd/get-strategy-class ks-def)))
    (is (= strategy-opts (kd/get-strategy-options ks-def)))
    (is (= "column-family-name" (-> (kd/get-cf-defs ks-def)
                                    first
                                    cfd/get-name)))
    (is (= "name" (-> (kd/get-cf-defs ks-def)
                      first
                      cfd/get-column-metadata
                      first
                      cd/get-name)))))

(deftest test-build-column-definition
  (let [name             "column-definition-test"
        validation-class "org.apache.cassandra.db.marshal.BytesType"
        cdef             (cd/build-cd name validation-class)]
    (is (= name (cd/get-name cdef)))
    (is (= validation-class (cd/get-validation-class cdef)))))

(deftest test-build-column-family-definition
  (let [keyspace           "CassaforteTest2"
        name               "column-family-name"
        column-definitions [(cd/build-cd "birth_date" "LongType") (cd/build-cd "full_name" "UTF8Type")]
        cfdef              (cfd/build-cfd keyspace name column-definitions)]
    (is (= keyspace (cfd/get-keyspace cfdef)))
    (is (= name (cfd/get-name cfdef)))
    ;; Order is guaranteed by Comparator, so that test does have a predictable order
    (is (= "birth_date" (cd/get-name (first (cfd/get-column-metadata cfdef)))))
    (is (= "full_name" (cd/get-name (second (cfd/get-column-metadata cfdef)))))))

(deftest t-build-column
  (let [name      "name"
        value     "John Doe"
        timestamp (System/currentTimeMillis)
        column    (c/build-column name value timestamp)
        c-map     (to-map column)]
    (are [expected actual] (is (= expected actual))
         name (deserialize "UTF8Type" (:name c-map))
         value (deserialize "UTF8Type" (:value c-map))
         timestamp (:timestamp c-map))))

(deftest t-build-super-column
  (let [key          "a0b1c2"
        column-map   {:age "26" :last_name "P" :first_name "Alex"}
        timestamp    (System/currentTimeMillis)
        super-column (sc/build-sc key column-map timestamp)
        sc-map       (to-map super-column)
        columns      (:columns sc-map)]
    (is (= key (:name sc-map)))
    (are [expected actual] (is (= expected (deserialize "UTF8Type" actual)))
         "age" (:name (first columns))
         "26" (:value (first columns))
         "last_name" (:name (second columns))
         "P" (:value (second columns))
         "first_name" (:name (nth columns 2))
         "Alex" (:value (nth columns 2)))))

(deftest t-to-plain-hash
  (is (= {:first "first" :second "second" :third "third"}
         (to-plain-hash [{:name :first, :value "first" :timestamp 1344930050150}
                         {:name :second, :value "second" :timestamp 1344930050150}
                         {:name :third, :value "third" :timestamp 1344930050150}])))

  (is (= {:name1 {:first "a" :second "b"}
          :name2 {:first "c" :second "d"}}
         (to-plain-hash
          [{:name "name1" :columns [{:name :first :value "a" :timestamp 1344929814489}
                                     {:name :second :value "b" :timestamp 1344929814489}]}
           {:name "name2" :columns [{:name :first :value "c" :timestamp 1344929814491}
                                     {:name :second :value "d" :timestamp 1344929814491}]}]))))