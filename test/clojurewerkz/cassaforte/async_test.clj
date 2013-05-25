(ns clojurewerkz.cassaforte.async-test
  (:require [clojurewerkz.cassaforte.test-helper :as th])
  (:use clojurewerkz.cassaforte.cql
        clojure.test
        clojurewerkz.cassaforte.query))

(use-fixtures :each th/initialize!)

(deftest async-test
  (th/test-combinations
   (let [record {:name "Alex" :city "Munich" :age (int 19)}
         result (atom nil)]
     (insert :users record)

     (set-callbacks
      (async (select :users))
      :success (fn [r] (reset! result r)))

     (Thread/sleep 20)
     (is (= record (first @result)))

     (is (= record
            (first (get-result (async (select :users)))))))))
