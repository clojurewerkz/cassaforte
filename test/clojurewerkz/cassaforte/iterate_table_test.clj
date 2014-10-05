(ns clojurewerkz.cassaforte.iterate-table-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojure.test :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))

(let [s (client/connect ["127.0.0.1"])]
  (use-fixtures :each (fn [f]
                        (th/with-temporary-keyspace s f)))

  (deftest test-iterate-table-with-natural-iteration-termination
    (let [n 100000]
      (dotimes [i n]
        (insert s :users {:name (str "name_" i) :city (str "city" i) :age (int i)}))

      (let [res (group-by :name
                          (iterate-table s :users :name 16384))]
        (dotimes [i n]
          (let [item (first (get res (str "name_" i)))]
            (is (= {:name (str "name_" i) :city (str "city" i) :age (int i)}
                   item)))))))

  (deftest test-iterate-table-with-explicit-limit
    (let [n 100]
      (dotimes [i n]
        (insert s :users {:name (str "name_" i) :city (str "city" i) :age (int i)}))

      (let [res (group-by :name
                          (iterate-table s :users :name 1024))]
        (dotimes [i n]
          (let [item (first (get res (str "name_" i)))]
            (is (= {:name (str "name_" i) :city (str "city" i) :age (int i)}
                   item))))))))
