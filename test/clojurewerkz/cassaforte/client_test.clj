(ns clojurewerkz.cassaforte.client-test
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojure.test :refer :all]))

(let [s (client/connect ["127.0.0.1"])]
  (deftest test-disconnect
    (let [cluster (.getCluster s)]
      (is (= (.isClosed s) false))
      (client/disconnect s)
      (is (= (.isClosed s) true))
      (is (= (.isClosed cluster) false))
      (client/shutdown-cluster cluster)
      (is (= (.isClosed cluster) true)))))


