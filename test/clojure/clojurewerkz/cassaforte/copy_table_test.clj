(ns clojurewerkz.cassaforte.copy-table-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojure.test :refer :all]
            ))

(use-fixtures :each (fn [f]
                      (th/with-temporary-keyspace f)))

;; (let [s (th/make-test-session)]
;;   (deftest test-copy-table-with-natural-iteration-termination
;;     (let [n 500]
;;       (dotimes [i n]
;;         (insert s :users {:name (str "name_" i) :city (str "city" i) :age (int i)}))

;;       (truncate s :users2)
;;       (is (= 0 (perform-count s :users2)))
;;       (copy-table s :users :users2 :name identity 16384)
;;       (is (= n (perform-count s :users2)))

;;       (dotimes [i n]
;;         (let [k (str "name_" i)
;;               a (first (select s :users  {:name k}))
;;               b (first (select s :users2 {:name k}))]
;;           (is (= a b)))))))
