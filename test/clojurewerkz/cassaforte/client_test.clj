(ns clojurewerkz.cassaforte.client-test
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.thrift.schema :as sch])
  (:use clojure.test
        clojurewerkz.cassaforte.utils)

  (:import clojurewerkz.cassaforte.CassandraClient))

(def ^String keyspace "cassaforte_test_1")

(deftest test-connection-with-explicitly-specified-host
  (let [host   (or (System/getenv "CASSANDRA_HOST") "127.0.0.1")
        port   cc/default-port
        ^CassandraClient client (cc/connect host port)]
    (is (= host (.getHost client)))))

(deftest test-with-client-binding
  (testing "When client binding is correct"
      (let [client (cc/connect "localhost")]
        (cc/with-client client
          (sch/set-keyspace "cassaforte_test_1"))))
  (testing "When client binding is correct"
    (is (thrown?
         org.apache.cassandra.thrift.InvalidRequestException
         (let [client (cc/connect "localhost")]
           (cc/with-client client
             (sch/set-keyspace "NonExistingKeyspace")))))))