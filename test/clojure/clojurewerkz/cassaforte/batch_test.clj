(ns clojurewerkz.cassaforte.batch-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.policies :as cp]
            [clojurewerkz.cassaforte.cql :as cql :refer :all]
            [clojurewerkz.cassaforte.uuids :as uuids]
            [clojurewerkz.cassaforte.query :refer :all]

            [clojure.test :refer :all]

            [clj-time.core :refer [seconds ago before? date-time] :as tc]
            [clj-time.format :as tf]
            [clj-time.coerce :as cc]))

(use-fixtures :each (fn [f]
                      (th/with-temporary-keyspace f)))

(let [s (th/make-test-session)]
  (deftest test-insert-batch-with-ttl-without-prepared-statements
    (let [input [[{:name "Alex" :city "Munich" :age (int 19)} (using :ttl (int 350))]
                  [{:name "Alex" :city "Munich" :age (int 19)} (using :ttl (int 350))]]]
       (insert-batch s :users input)
       (is (= (first (first input)) (first (select s :users))))
       (truncate s :users)))

  (deftest test-insert-batch-plain-without-prepared-statements
    (let [input [{:name "Alex" :city "Munich" :age (int 19)}
                  {:name "Alex" :city "Munich" :age (int 19)}]]
       (insert-batch s :users input)
       (is (= (first input) (first (select s :users))))
       (truncate s :users)))

  (deftest test-insert-with-atomic-batch-without-prepared-statements
    (cql/atomic-batch s (queries
                          (hs/insert :users (values {:name "Alex" :city "Munich" :age (int 19)}))
                          (hs/insert :users (values {:name "Fritz" :city "Hamburg" :age (int 28)}))))
     (is (= "Munich" (-> (select s :users) first :city)))
     (truncate s :users))

  ;; there's some oddity with this test on C* 2.1, doesn't seem to be
  ;; a client problem. Needs more investigation. MK.
  #_ (deftest test-insert-with-atomic-batch-with-prepared-statements
    (client/prepared
     (cql/atomic-batch s (queries
                          (hs/insert :users (values {:name "Alex" :city "Munich" :age (int 19)}))
                          (hs/insert :users (values {:name "Fritz" :city "Hamburg" :age (int 28)}))))
     (is (= "Munich" (-> (select s :users) first :city)))
     (truncate s :users))))
