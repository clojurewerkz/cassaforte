(ns clojurewerkz.cassaforte.client-test
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.client :as client]
            [clojure.test :refer :all]))

(let [s (client/connect ["127.0.0.1"])]
  (use-fixtures :each (fn [f]
                        (th/with-temporary-keyspace s f)))

  (deftest test-disconnect
    (let [cluster (.getCluster s)]
      (is (= (.isClosed s) false))
      (client/disconnect s)
      (is (= (.isClosed s) true))
      (is (= (.isClosed cluster) false))
      (client/shutdown-cluster cluster)
      (is (= (.isClosed cluster) true)))))


