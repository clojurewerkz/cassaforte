(ns clojurewerkz.cassaforte.utils-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojure.test :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]
            [clojurewerkz.cassaforte.utils :refer :all]))

(let [s (client/connect ["127.0.0.1"])]
  (use-fixtures :each (fn [f]
                        (th/with-temporary-keyspace s f)))

  (deftest test-transform-dynamic-table
    (create-table s :time_series
                  (column-definitions {:metric       :varchar
                                       :time         :timestamp
                                       :value_1      :varchar
                                       :value_2      :varchar
                                       :primary-key [:metric :time]}))

    (insert s :time_series
            {:metric "metric1" :time 1368395185947 :value_1 "val_1_1" :value_2 "val_1_2"})

    (insert s :time_series
            {:metric "metric1" :time 1368396471276 :value_1 "val_2_1" :value_2 "val_2_2"})

    (let [res (transform-dynamic-table (select s :time_series)
                                       :metric :time)]

      (is (= {:value_1 "val_1_1" :value_2 "val_1_2"}
             (get-in res ["metric1" (java.util.Date. 1368395185947)])))
      (is (= {:value_1 "val_2_1" :value_2 "val_2_2"}
             (get-in res ["metric1" (java.util.Date. 1368396471276)]))))

    (drop-table s :time_series)))
