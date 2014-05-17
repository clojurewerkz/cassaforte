(ns clojurewerkz.cassaforte.iterate-table-test
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojure.test :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))

(let [s (client/connect ["127.0.0.1"])]
  (use-fixtures :each (fn [f]
                        (th/with-temporary-keyspace s f)))

  (deftest test-iterate-table
    (th/test-combinations
     (dotimes [i 100]
       (insert s :users {:name (str "name_" i) :city (str "city" i) :age (int i)})))

    (let [res (group-by :name
                        (take 100 (iterate-table s :users :name 10)))]
      (dotimes [i 100]
        (let [item (first (get res (str "name_" i)))]
          (is (= {:name (str "name_" i) :city (str "city" i) :age (int i)}
                 item)))))))
