(ns clojurewerkz.cassaforte.stress-test
  (:require [clojurewerkz.cassaforte.test-helper :as th])
  (:use clojurewerkz.cassaforte.cql
        clojure.test
        clojurewerkz.cassaforte.query))

(use-fixtures :each th/initialize!)

(deftest ^{:stress true :skip-ci true} insert-test
  (doseq [n [1000 10000 100000]]
    (let [table  :users
          docs   (map (fn [i]
                        {:name (str "user_" i) :city (str "city_" i) :age (int i)})
                      (take n (iterate inc 1)))]
      (truncate table)
      (println "Inserting " n " documents...")
      (time (insert-batch table docs))
      (is (= n (perform-count table))))))
