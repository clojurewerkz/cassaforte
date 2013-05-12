(ns clojurewerkz.cassaforte.iterate-world-test
  (:require [clojurewerkz.cassaforte.test-helper :as th])
  (:use clojurewerkz.cassaforte.cql
        clojurewerkz.cassaforte.utils
        clojure.test
        clojurewerkz.cassaforte.query))

(use-fixtures :each th/initialize!)

(deftest iterate-world-test
  (create-table :time_series
                (column-definitions {:metric       :varchar
                                     :time         :timestamp
                                     :value_1      :varchar
                                     :value_2      :varchar
                                     :primary-key [:metric :time]}))

  (insert :time_series
          {:metric "metric1" :time 1368395185947 :value_1 "val_1_1" :value_2 "val_1_2"})

  (insert :time_series
          {:metric "metric1" :time 1368396471276 :value_1 "val_2_1" :value_2 "val_2_2"})

  (let [res (transform-dynamic-table (select :time_series)
                                     :metric :time)]

    (is (= {:value_1 "val_1_1" :value_2 "val_1_2"}
           (get-in res ["metric1" (java.util.Date. 1368395185947)])))
    (is (= {:value_1 "val_2_1" :value_2 "val_2_2"}
           (get-in res ["metric1" (java.util.Date. 1368396471276)]))))


  (drop-table :time_series))
