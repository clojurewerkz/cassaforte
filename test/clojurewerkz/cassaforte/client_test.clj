(ns clojurewerkz.cassaforte.client-test
  (:require [clojurewerkz.cassaforte.client :as cc])
  (:use clojure.test)
  (:import clojurewerkz.cassaforte.CassandraClient))

(def ^String keyspace "CassaforteTest1")

(deftest test-connection-with-explicitly-specified-host
  (let [host   (or (System/getenv "CASSANDRA_HOST") "127.0.0.1")
        port   cc/default-port
        ^CassandraClient client (cc/connect host port)]
    (is (= host (.getHost client)))))

(deftest test-with-client-binding
  (testing "When client binding is correct"
      (let [client (cc/connect "localhost")]
        (cc/with-client client
          (cc/set-keyspace! "CassaforteTest1"))))
  (testing "When client binding is correct"
    (is (thrown?
         org.apache.cassandra.thrift.InvalidRequestException
         (let [client (cc/connect "localhost")]
           (cc/with-client client
             (cc/set-keyspace! "NonExistingKeyspace")))))))