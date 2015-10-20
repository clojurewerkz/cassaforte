(ns clojurewerkz.cassaforte.iterate-table-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :refer [*session* with-table with-temporary-keyspace]]
            [clojurewerkz.cassaforte.client      :as client]
            [clojurewerkz.cassaforte.cql         :refer :all]
            [clojure.test                        :refer :all]))

(use-fixtures :each with-temporary-keyspace)

(deftest test-iterate-table-with-natural-iteration-termination
  (let [n 100000]
    (dotimes [i n]
      (insert *session* :users {:name (str "name_" i)
                                :city (str "city" i)
                                :age  (int i)}))

    (let [res (group-by :name
                        (iterate-table *session* :users :name 16384))]
      (dotimes [i n]
        (let [item (first (get res (str "name_" i)))]
          (is (= {:name (str "name_" i)
                  :city (str "city" i)
                  :age  (int i)}
                 item)))))))

(deftest test-iterate-table-with-explicit-limit
  (let [n 100]
    (dotimes [i n]
      (insert *session* :users {:name (str "name_" i) :city (str "city" i) :age (int i)}))

    (let [res (group-by :name
                        (iterate-table *session* :users :name 1024))]
      (dotimes [i n]
        (let [item (first (get res (str "name_" i)))]
          (is (= {:name (str "name_" i)
                  :city (str "city" i)
                  :age  (int i)}
                 item)))))))
