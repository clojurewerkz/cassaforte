(ns clojurewerkz.cassaforte.copy-table-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client      :as client]
            [clojurewerkz.cassaforte.test-helper :refer [*session* with-table with-temporary-keyspace]]
            [clojurewerkz.cassaforte.cql         :refer :all]
            [clojure.test                        :refer :all]
            ))

(use-fixtures :each with-temporary-keyspace)

(deftest test-copy-table-with-natural-iteration-termination
  (let [n 500]
    (dotimes [i n]
      (insert *session* :users {:name (str "name_" i) :city (str "city" i) :age (int i)}))

    (truncate *session* :users2)
    (is (= 0 (perform-count *session* :users2)))
    (copy-table *session* :users :users2 :name identity 16384)
    (is (= n (perform-count *session* :users2)))

    (dotimes [i n]
      (let [k (str "name_" i)
            a (first (select *session* :users
                             (where {:name k})))
            b (first (select *session* :users2
                             (where {:name k})))]
        (is (= a b))))))
