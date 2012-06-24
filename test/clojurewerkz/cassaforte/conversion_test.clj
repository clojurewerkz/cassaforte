(ns clojurewerkz.cassaforte.system-test
  (:use clojure.test
        clojurewerkz.cassaforte.conversion)
  (:import [org.apache.cassandra.thrift ConsistencyLevel]))


(deftest test-to-consistency-level
  (testing "with keyword values"
    (are [a b] (is (= a (to-consistency-level b)))
         ConsistencyLevel.ANY :any
         ConsistencyLevel.ONE :ONE
         ConsistencyLevel.TWO :TWO
         ConsistencyLevel.THREE :three
         ConsistencyLevel.QUORUM :quorum
         ConsistencyLevel.LOCAL_QUORUM :local_quorum
         ConsistencyLevel.EACH_QUORUM :each_quorum
         ConsistencyLevel.ALL :all))
  (testing "with consistency level enum values"
    (are [a] (is (= a (to-consistency-level a)))
         ConsistencyLevel.ANY
         ConsistencyLevel.ONE
         ConsistencyLevel.TWO
         ConsistencyLevel.THREE
         ConsistencyLevel.QUORUM
         ConsistencyLevel.LOCAL_QUORUM
         ConsistencyLevel.EACH_QUORUM
         ConsistencyLevel.ALL))
  (testing "with string values"
    (are [a b] (is (= a (to-consistency-level b)))
         ConsistencyLevel.ANY "any"
         ConsistencyLevel.ONE "ONE"
         ConsistencyLevel.TWO "TWO"
         ConsistencyLevel.THREE "three"
         ConsistencyLevel.QUORUM "quorum"
         ConsistencyLevel.LOCAL_QUORUM "local_quorum"
         ConsistencyLevel.EACH_QUORUM "each_quorum"
         ConsistencyLevel.ALL "all")))
