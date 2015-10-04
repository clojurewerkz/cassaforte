(ns clojurewerkz.cassaforte.async-cql-test
  (:refer-clojure :exclude [update])
  (:import [java.util.concurrent CountDownLatch Executors])
  )


;;   (use-fixtures :each (fn [f]
;;                         (th/with-temporary-keyspace f)))

;; (let [s (th/make-test-session)]
;;   (deftest test-insert
;;     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
;;       (deref (client/async
;;               (insert s :users r)))
;;       (is (= r (first @(select-async s :users))))
;;       (truncate s :users)))

;;   (deftest test-insert-time-unit
;;     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
;;       @(insert-async s :users r)
;;       (is (= r (first (deref (select-async s :users) 1000 nil))))
;;       (truncate s :users)))

;;   (deftest test-insert-callback
;;     (let [r     {:name "Alex" :city "Munich" :age (int 19)}
;;           latch (CountDownLatch. 1)]
;;       @(insert-async s :users r)
;;       (let [result-future (select-async s :users)]
;;         (client/add-listener result-future
;;                              (fn [] (.countDown latch))
;;                              (Executors/newFixedThreadPool 1))
;;         (is (= r (first @result-future)))
;;         (.await latch)
;;         (is (= 0 (.getCount latch))))

;;       (truncate s :users)))

;;   (deftest test-insert-batch-with-ttl
;;     (let [input [[{:name "Alex" :city "Munich" :age (int 19)} (using :ttl (int 350))]
;;                  [{:name "Alex" :city "Munich" :age (int 19)} (using :ttl (int 350))]]]
;;       @(insert-batch-async s :users input)

;;       @(insert-batch-async s :users input)
;;       (is (= (first (first input)) (first @(select-async s :users))))
;;       (truncate s :users)))

;;   (deftest test-insert-batch-plain
;;     (let [input [{:name "Alex" :city "Munich" :age (int 19)}
;;                  {:name "Alex" :city "Munich" :age (int 19)}]]
;;       @(insert-batch-async s :users input)
;;       (is (= (first input) (first @(select-async s :users))))
;;       (truncate s :users)))

;;   (deftest test-update
;;     (testing "Simple updates"
;;       (let [r {:name "Alex" :city "Munich" :age (int 19)}]
;;         @(insert-async s :users r)
;;         (is (= r (first @(select-async s :users))))
;;         (update s :users
;;                 {:age (int 25)}
;;                 (where {:name "Alex"}))
;;         (is (= {:name "Alex" :city "Munich" :age (int 25)}
;;                (first @(select-async s :users))))))

;;     (testing "One of many update"
;;       (dotimes [i 3]
;;         @(insert-async s :user_posts {:username "user1"
;;                                       :post_id (str "post" i)
;;                                       :body (str "body" i)}))
;;       @(update-async s :user_posts
;;                      {:body "bodynew"}
;;                      (where {:username "user1"
;;                              :post_id "post1"}))
;;       (is (= "bodynew"
;;              (get-in @(select-async s :user_posts
;;                                     (where {:username "user1"
;;                                             :post_id "post1"}))
;;                      [0 :body])))))

;;   (deftest test-delete
;;     (->> (range 0 3)
;;          (map #(insert-async s :users {:name (str "name" %) :age (int %)}))
;;          (map deref)
;;          doall) ;; Better deref before couting, otherwise there's a good chance it's not even inserted yet


;;     (is (= 3 (perform-count s :users)))
;;     @(delete-async s :users
;;                    (where {:name "name1"}))
;;     (is (= 2 (perform-count s :users)))
;;     (truncate s :users)

;;     @(insert-async s :users {:name "name1" :age (int 19)})
;;     @(delete-async s :users
;;                    (columns :age)
;;                    (where {:name "name1"}))
;;     (is (nil? (:age @(select-async s :users))))
;;     (truncate s :users))

;;   (deftest test-insert-with-timestamp
;;     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
;;       @(insert-async s :users r
;;                      (using :timestamp (.getTime (java.util.Date.))))
;;       (is (= r (first @(select-async s :users))))
;;       (truncate s :users))))
