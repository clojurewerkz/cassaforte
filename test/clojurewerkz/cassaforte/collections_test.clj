(ns clojurewerkz.cassaforte.collections-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.cql :as cql :refer :all]
            [clojure.test :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))

(let [s (client/connect ["127.0.0.1"])]
  (use-fixtures :each (fn [f]
                        (th/with-temporary-keyspace s f)))
  (deftest test-collection-conversion-on-load
    (let [t :users_collections]
      (create-table s t
                  (column-definitions
                   {:name :varchar
                    :test_map (map-type :varchar :varchar)
                    :test_set (set-type :int)
                    :test_list (list-type :varchar)
                    :primary-key [:name]}))

    (insert s t
            {:name "user1"
             :test_map {"a" "b" "c" "d"}
             :test_set #{1 2 3}
             :test_list ["clojure" "cassandra"]})
    (let [m  (-> (select s t) first)
          mv (:test_map m)
          ms (:test_set m)
          ml (:test_list m)]
      (is (map? mv))
      (is (= {"a" "b" "c" "d"} mv))
      (is (set? ms))
      (is (= #{1 2 3} ms))
      (is (vector? ml))
      (is (= ["clojure" "cassandra"] ml)))
    (truncate s t)
    (drop-table s t))))
