(ns clojurewerkz.cassaforte.thrift.schema-test
  (:refer-clojure :exclude [get])
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.thrift.schema :as sch]
            [clojurewerkz.cassaforte.conversion :as conv]
            [clojurewerkz.cassaforte.ddl.keyspace-definition :as kd]
            [clojurewerkz.cassaforte.ddl.column-definition :as cd]

            [clojurewerkz.cassaforte.ddl.column-family-definition :as cfd]
            [clojurewerkz.cassaforte.ddl.column-or-super-column :as cosc]
            [clojurewerkz.cassaforte.cql :as cql])
  (:use clojurewerkz.cassaforte.test-helper
        clojurewerkz.cassaforte.utils
        clojurewerkz.cassaforte.conversion
        clojure.test))

(use-fixtures :once initialize-thrift)

(deftest ^{:schema true}  t-add-describe-keyspace
  (let [keyspace "add_keyspace_test"]
    (with-thrift-exception-handling (sch/drop-keyspace keyspace))
    (sch/add-keyspace
     keyspace
     "org.apache.cassandra.locator.SimpleStrategy"
     [(cfd/build-cfd "ColumnFamily2" [(cd/build-cd "first" "UTF8Type")
                                      (cd/build-cd "second" "UTF8Type")
                                      (cd/build-cd "third" "UTF8Type")])]
     :strategy-opts {"replication_factor" "1"})

    (let [ks (sch/describe-keyspace keyspace)]
      (is (= "org.apache.cassandra.locator.SimpleStrategy" (:strategy-class ks)))
      (is (= keyspace (:name ks)))
      (is (= 1 (count (:cf-defs ks))))
      (is (= 3 (count (:column-metadata (first (:cf-defs ks)))))))
    (sch/drop-keyspace keyspace)))


(deftest ^{:schema true}  t-add-update-describe-keyspace
  (let [keyspace "add_update_describe_keyspace_test"]
    (with-thrift-exception-handling (sch/drop-keyspace keyspace))
    (sch/add-keyspace keyspace
                      "org.apache.cassandra.locator.SimpleStrategy"
                      []
                      :strategy-opts {"replication_factor" "1"})

    (with-thrift-exception-handling
      (sch/update-keyspace keyspace
                           "org.apache.cassandra.locator.NetworkTopologyStrategy"
                           :strategy-opts {"us_east" "6" "us_west" "3"})


      (let [ks (sch/describe-keyspace keyspace)]
        (is (= "org.apache.cassandra.locator.NetworkTopologyStrategy" (:strategy-class ks)))
        (is (= {"us_west" "3", "us_east" "6"} (:strategy-opts ks)))))

    (sch/drop-keyspace keyspace)))

(deftest ^{:schema true} test-add-and-drop-keyspace
  (let [keyspace       "CassaforteTest2"
        strategy-class "org.apache.cassandra.locator.SimpleStrategy"
        strategy-opts  {:replication_factor "1"}
        cf-defs        [(cfd/build-cfd "movies")]]
    (with-thrift-exception-handling (sch/drop-keyspace keyspace))
    (is (sch/add-keyspace keyspace strategy-class cf-defs :strategy-opts strategy-opts))
    (is (sch/drop-keyspace keyspace))))

(deftest ^{:schema true} test-add-and-drop-keyspace
  (let [keyspace       "CassaforteTest2"
        strategy-class "org.apache.cassandra.locator.SimpleStrategy"
        strategy-opts  {:replication_factor "1"}
        cf-defs        [(cfd/build-cfd "movies")]]
    (is (sch/add-keyspace keyspace strategy-class cf-defs :strategy-opts strategy-opts))
    (is (sch/drop-keyspace keyspace))))

(deftest ^{:schema true}  test-describe-keyspace
  (let [keyspace       "CassaforteTest2"
        strategy-class "org.apache.cassandra.locator.SimpleStrategy"
        strategy-opts  {:replication_factor "1"}
        cf-defs        [(cfd/build-cfd "movies")]]
    (sch/add-keyspace keyspace strategy-class cf-defs :strategy-opts strategy-opts)
    (let [ksdef (to-map (sch/describe-keyspace keyspace))]
      (is (= strategy-class (:strategy-class ksdef)))
      (is (= keyspace (:name ksdef))))
    (is (sch/drop-keyspace keyspace))))



(deftest ^{:schema true}  t-composite-columns
  (let [keyspace "add_keyspace_test"]
    (with-thrift-exception-handling (sch/drop-keyspace keyspace))
    (sch/add-keyspace
     keyspace
     "org.apache.cassandra.locator.SimpleStrategy"
     [(cfd/build-cfd "ColumnFamily2" [(cd/build-cd "first" "ListType(UTF8Type)")])]
     :strategy-opts {"replication_factor" "1"})

        (sch/drop-keyspace keyspace)))
