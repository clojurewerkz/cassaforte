(ns clojurewerkz.cassaforte.async-cql-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.policies :as cp]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojure.test :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))

(let [s (th/make-test-session)]
  (use-fixtures :each (fn [f]
                        (th/with-temporary-keyspace s f)))

  (deftest test-insert
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (insert-async s :users r)
       (is (= r (first @(select-async s :users))))
       (truncate s :users))))

  (deftest test-insert-batch-with-ttl
    (th/test-combinations
     (let [input [[{:name "Alex" :city "Munich" :age (int 19)} (using :ttl (int 350))]
                  [{:name "Alex" :city "Munich" :age (int 19)} (using :ttl (int 350))]]]
       (insert-batch-async s :users input)
       (is (= (first (first input)) (first @(select-async s :users))))
       (truncate s :users))))

  (deftest test-insert-batch-plain
    (th/test-combinations
     (let [input [{:name "Alex" :city "Munich" :age (int 19)}
                  {:name "Alex" :city "Munich" :age (int 19)}]]
       (insert-batch-async s :users input)
       (is (= (first input) (first @(select-async s :users))))
       (truncate s :users))))

  (deftest test-update
    (testing "Simple updates"
      (th/test-combinations
       (let [r {:name "Alex" :city "Munich" :age (int 19)}]
         (insert-async s :users r)
         (is (= r (first @(select-async s :users))))
         (update s :users
                 {:age (int 25)}
                 (where {:name "Alex"}))
         (is (= {:name "Alex" :city "Munich" :age (int 25)}
                (first @(select-async s :users)))))))

    (testing "One of many update"
      (th/test-combinations
       (dotimes [i 3]
         (insert-async s :user_posts {:username "user1"
                                      :post_id (str "post" i)
                                      :body (str "body" i)}))
       (update-async s :user_posts
                     {:body "bodynew"}
                     (where {:username "user1"
                             :post_id "post1"}))
       (is (= "bodynew"
              (get-in @(select-async s :user_posts
                                     (where {:username "user1"
                                             :post_id "post1"}))
                      [0 :body]))))))

  (deftest test-delete
    (th/test-combinations
     (dotimes [i 3]
       (insert-async s :users {:name (str "name" i) :age (int i)}))
     (is (= 3 (perform-count s :users)))
     (delete-async s :users
                   (where {:name "name1"}))
     (is (= 2 (perform-count s :users)))
     (truncate s :users))

    (th/test-combinations
     (insert-async s :users {:name "name1" :age (int 19)})
     (delete-async s :users
                   (columns :age)
                   (where {:name "name1"}))
     (is (nil? (:age @(select-async s :users))))
     (truncate s :users)))

  (deftest test-insert-with-timestamp
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (insert-async s :users r
                     (using :timestamp (.getTime (java.util.Date.))))
       (is (= r (first @(select-async s :users))))
       (truncate s :users))))

  (deftest test-select-async-with-callbacks
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}
           success (fn [res]
                     (is (= r (first res)))
                     (truncate s :users))]
       (is (= [] @(insert-async s :users r)))
       (client/set-callbacks (select-async s :users) {:success success})))))
