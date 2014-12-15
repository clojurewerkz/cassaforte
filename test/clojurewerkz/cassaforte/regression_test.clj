(ns clojurewerkz.cassaforte.regression-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.cql :as cql :refer :all]
            [clojure.test :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))

(let [s (client/connect ["127.0.0.1"])]
  (use-fixtures :each (fn [f]
                        (th/with-temporary-keyspace s f)))

  (deftest test-issue91
    (th/test-combinations
     (let [m1 {:name "Alex"    :city "Munich" :age (int 28)}
           m2 {:name "Michael" :city ""       :age (int 30)}
           m3 {:name "Joe"     :city nil      :age (int 30)}]
       (insert   s :users m1)
       (insert   s :users m2)
       (insert   s :users m3)
       (is (.isEmpty (-> (select s :users (where [[= :name "Michael"]]))
                         first :city)))
       (are [k m] (is (= m (first (select s :users (where [[= :name k]])))))
            "Alex"    m1
            "Michael" m2
            "Joe"     m3)
       (truncate s :users)))))
