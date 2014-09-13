(ns clojurewerkz.cassaforte.client-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojure.test :refer :all]))


(deftest ^:client test-disconnect
  (let [s (client/connect ["127.0.0.1"])
        cluster (.getCluster s)]
    (is (= (.isClosed s) false))
    (client/disconnect s)
    (is (= (.isClosed s) true))
    (is (= (.isClosed cluster) false))
    (client/shutdown-cluster cluster)
    (is (= (.isClosed cluster) true))))


(deftest ^:client test-disconnect!
  (let [s (client/connect ["127.0.0.1"])
        cluster (.getCluster s)]
    (is (= (.isClosed s) false))
    (is (= (.isClosed cluster) false))
    (client/disconnect! s)
    (is (= (.isClosed s) true))
    (is (= (.isClosed cluster) true))))



