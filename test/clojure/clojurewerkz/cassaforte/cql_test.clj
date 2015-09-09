(ns clojurewerkz.cassaforte.cql-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.client      :as client]
            [clojurewerkz.cassaforte.policies    :as cp]
            [clojurewerkz.cassaforte.uuids       :as uuids]

            [clj-time.format                     :as tf]
            [clj-time.coerce                     :as cc]
            [clj-time.core                       :refer [seconds ago before? date-time] :as tc]

            ;; [clojurewerkz.cassaforte.query       :refer :all]
            [clojurewerkz.cassaforte.cql         :refer :all]

            [clojure.test                        :refer :all]
            [clojurewerkz.cassaforte.new-query-api :as new-query-api]
            [clojurewerkz.cassaforte.query.dsl :refer :all]
            ))


(use-fixtures :each th/with-temporary-keyspace)


(let [s (th/make-test-session)]
  (deftest test-insert
    (let [r {:name "Alex" :city "Munich" :age (int 19)}]
      (insert s :users r)
      (is (= r (first (select s :users))))
      (truncate s :users)
      ))

  (deftest test-insert-prepare
    (let [prepared (client/prepare
                    (insert s :users
                            {:name new-query-api/?
                             :city new-query-api/?
                             :age  new-query-api/?}))
          r        {:name "Alex" :city "Munich" :age (int 19)}]
      (client/execute s
                      (client/bind prepared ["Alex" "Munich" (int 19)]))
      (is (= r (first (select s :users))))))

  (deftest test-update
    (testing "Simple updates"
      (let [r {:name "Alex" :city "Munich" :age (int 19)}]
        (insert s :users r)
        (is (= r (first (select s :users))))
        (update s :users
                {:age (int 25)}
                (where {:name "Alex"}))
        (is (= {:name "Alex" :city "Munich" :age (int 25)}
               (first (select s :users))))))

    (testing "One of many update"
      (dotimes [i 3]
        (insert s :user_posts {:username "user1"
                               :post_id (str "post" i)
                               :body (str "body" i)}))

      (update s :user_posts
              {:body "bodynew"}
              (where {:username "user1"
                      :post_id "post1"}))
      (is (= "bodynew"
             (get-in
              (select s :user_posts
                      (where {:username "user1"
                              :post_id "post1"}))
              [0 :body]))))))

(comment




  (deftest test-update-with-compound-key
    (let [t   :events_by_device_id_and_date
          fmt (tf/formatter "yyyy-MM-dd")
          id  "device-000000001"
          qc  [[=  :device_id id]
               [=  :date "2014-11-13"]
               [=  :created_at (uuids/start-of (cc/to-long (date-time 2014 11 13 12)))]]]
      (testing "Bulk update"
        (truncate s t)
        (is (= 0 (perform-count s t)))
        (doseq [i  (range 1 15)]
          (let [dt (date-time 2014 11 i 12)
                td (uuids/start-of (cc/to-long dt))]

            (insert s t {:created_at td
                         :device_id  id
                         :date       (tf/unparse fmt dt)
                         :payload    (str "body" i)})))
        (is (= 14 (perform-count s t)))
        (is (= 14 (count (select s t))))
        (is (= 1  (count (select s t (where qc)))))
        (update s t
                {:payload "updated payload"}
                (where qc))
        (let [x (first (select s t (where qc)))]
          (is (= "updated payload" (:payload x))))
        (truncate s t))))

  (deftest test-delete
    (testing "Delete whole row"
      (dotimes [i 3]
        (insert s :users {:name (str "name" i) :age (int i)}))
      (is (= 3 (perform-count s :users)))
      (delete s :users
              (where {:name "name1"}))
      (is (= 2 (perform-count s :users)))
      (truncate s :users))

    (testing "Delete a column"
      (insert s :users {:name "name1" :age (int 19)})
      (delete s :users
              (columns :age)
              (where {:name "name1"}))
      (is (nil? (:age (select s :users))))
      (truncate s :users)))

  (deftest test-insert-with-timestamp
    (let [r {:name "Alex" :city "Munich" :age (int 19)}]
      (insert s :users r
              (using :timestamp (.getTime (java.util.Date.))))
      (is (= r (first (select s :users))))
      (truncate s :users)))

  (deftest test-ttl
    (dotimes [i 3]
      (insert s :users {:name (str "name" i) :city (str "city" i) :age (int i)}
              (using :ttl (int 2))))
    (is (= 3 (perform-count s :users)))
    (Thread/sleep 2100)
    (is (= 0 (perform-count s :users))))


  (deftest test-counter
    (update s :user_counters {:user_count (increment-by 5)} (where {:name "user1"}))
    (is (= 5 (:user_count (first (select s :user_counters)))))
    (update s :user_counters {:user_count (decrement-by 5)} (where {:name "user1"}))
    (is (= 0 (:user_count (first (select s :user_counters)))))
    (update s :user_counters {:user_count [+ 500]} (where {:name "user1"}))
    (is (= 500 (:user_count (first (select s :user_counters)))))
    (update s :user_counters {:user_count [+ 5000000]} (where {:name "user1"}))
    (is (= 5000500 (:user_count (first (select s :user_counters)))))
    (update s :user_counters {:user_count [+ 50000000000000]} (where {:name "user1"}))
    (is (= 50000005000500 (:user_count (first (select s :user_counters))))))

  (deftest test-counter-2
    (dotimes [i 100]
      (update s :user_counters {:user_count [+ 1]} (where {:name "user1"})))
    (is (= 100 (:user_count (first (select s :user_counters))))))

  (deftest test-index-filtering-range
    (create-index s :users :city)
    (create-index s :users :age)
    (dotimes [i 10]
      (insert s :users {:name (str "name_" i) :city "Munich" :age (int i)}))

    (let [res (select s :users
                      (where [[= :city "Munich"]
                              [> :age (int 5)]])
                      (allow-filtering true))]
      (is (= (set (range 6 10))
             (->> res
                  (map :age)
                  set))))
    (truncate s :users))

  (deftest test-index-filtering-range-alt-syntax
    (create-index s :users :city)
    (create-index s :users :age)
    (dotimes [i 10]
      (insert s :users {:name (str "name_" i) :city "Munich" :age (int i)}))

    (let [res (select s :users
                      (where [[= :city "Munich"]
                              [> :age (int 5)]])
                      (allow-filtering true))]
      (is (= (set (range 6 10))
             (->> res
                  (map :age)
                  set))))
    (truncate s :users))

  (deftest test-index-exact-match
    (create-index s :users :city)
    (dotimes [i 10]
      (insert s :users {:name (str "name_" i) :city (str "city_" i) :age (int i)}))


    (let [res (select s :users
                      (where {:city "city_5"})
                      (allow-filtering true))]
      (is (= 5
             (-> res
                 first
                 :age))))
    (truncate s :users))

  (deftest test-select-where
    (insert s :users {:name "Alex"   :city "Munich"        :age (int 19)})
    (insert s :users {:name "Robert" :city "Berlin"        :age (int 25)})
    (insert s :users {:name "Sam"    :city "San Francisco" :age (int 21)})

    (is (= "Munich" (get-in (select s :users (where {:name "Alex"})) [0 :city]))))

  (deftest test-select-where-without-fetch
    (insert s :users {:name "Alex"   :city "Munich"        :age (int 19)})
    (insert s :users {:name "Robert" :city "Berlin"        :age (int 25)})
    (insert s :users {:name "Sam"    :city "San Francisco" :age (int 21)})

    (is (= "Munich" (get-in (client/execute s
                                            "SELECT * FROM users where name='Alex';"
                                            :fetch-size Integer/MAX_VALUE) [0 :city]))))

  (deftest test-select-in
    (insert s :users {:name "Alex"   :city "Munich"        :age (int 19)})
    (insert s :users {:name "Robert" :city "Berlin"        :age (int 25)})
    (insert s :users {:name "Sam"    :city "San Francisco" :age (int 21)})

    (let [users (select s :users
                        (where [[:in :name ["Alex" "Robert"]]]))]
      (is (= "Munich" (get-in users [0 :city])))
      (is (= "Berlin" (get-in users [1 :city])))))

  (deftest test-select-order-by
    (dotimes [i 3]
      (insert s :user_posts {:username "Alex" :post_id  (str "post" i) :body (str "body" i)}))

    (is (= [{:post_id "post0"}
            {:post_id "post1"}
            {:post_id "post2"}]
           (select s :user_posts
                   (columns :post_id)
                   (where {:username "Alex"})
                   (order-by [:post_id]))))

    (is (= [{:post_id "post2"}
            {:post_id "post1"}
            {:post_id "post0"}]
           (select s :user_posts
                   (columns :post_id)
                   (where {:username "Alex"})
                   (order-by [:post_id :desc])))))

  (deftest test-select-range-query
    (create-table s :tv_series
                  (column-definitions {:series_title  :varchar
                                       :episode_id    :int
                                       :episode_title :text
                                       :primary-key   [:series_title :episode_id]}))
    (dotimes [i 20]
      (insert s :tv_series {:series_title "Futurama" :episode_id i :episode_title (str "Futurama Title " i)})
      (insert s :tv_series {:series_title "Simpsons" :episode_id i :episode_title (str "Simpsons Title " i)}))

    (is (= (set (range 11 20))
           (->> (select s :tv_series
                        (where [[= :series_title "Futurama"]
                                [> :episode_id 10]]))
                (map :episode_id )
                set)))

    (is (= (set (range 11 16))
           (->> (select s :tv_series
                        (where [[=  :series_title "Futurama"]
                                [>  :episode_id 10]
                                [<= :episode_id 15]]))
                (map :episode_id)
                set)))

    (is (= (set (range 0 15))
           (->> (select s :tv_series
                        (where [[= :series_title "Futurama"]
                                [< :episode_id 15]]))
                (map :episode_id)
                set)))

    (drop-table s :tv_series))

  (deftest test-timeuuid-now-and-unix-timestamp-of
    (create-table s :events
                  (column-definitions {:message      :varchar
                                       :created_at   :timeuuid
                                       :primary-key  [:created_at]}))

    (let [dt (-> 2 seconds ago)
          ts (cc/to-long dt)]
      (dotimes [i 20]
        (insert s :events {:created_at (fns/now) :message (format "Message %d" i)}))
      (let [xs  (select s :events
                        (columns (fns/unix-timestamp-of :created_at))
                        (limit 5))
            ts' (get (first xs) (keyword "unixTimestampOf(created_at)"))]
        (is (> ts' ts))))

    (drop-table s :events))

  (deftest test-timeuuid-dateof
    (create-table s :events
                  (column-definitions {:message      :varchar
                                       :created_at   :timeuuid
                                       :primary-key  [:created_at]}))

    (let [dt (-> 2 seconds ago)]
      (dotimes [i 20]
        (insert s :events {:created_at (fns/now) :message (format "Message %d" i)}))
      (let [xs  (select s :events
                        (columns (fns/date-of :created_at))
                        (limit 5))
            dt' (cc/from-date (get (first xs) (keyword "dateOf(created_at)")))]
        (is (before? dt dt'))))

    (drop-table s :events))

  (deftest test-timeuuid-min-open-range-query
    (create-table s :events
                  (column-definitions {:message      :varchar
                                       :city         :varchar
                                       :created_at   :timeuuid
                                       :primary-key  [:message :created_at]}))
    (create-index s :events :created_at)
    (dotimes [i 10]
      (let [dt (date-time 2014 11 17 23 i)]
        (insert s :events {:created_at (uuids/start-of (cc/to-long dt))
                           :city       "London, UK"
                           :message    (format "Message %d" i)})))
    (let [xs  (select s :events
                      (where [[:in :message ["Message 7" "Message 8" "Message 9" "Message 10"]]
                              [>= :created_at (-> (date-time 2014 11 17 23 8)
                                                  .toDate
                                                  fns/min-timeuuid)]]))]
      (is (= #{"Message 8" "Message 9"}
             (set (map :message xs)))))

    (drop-table s :events))

  (deftest test-timeuuid-min-max-timeuuid-range-query
    (create-table s :events
                  (column-definitions {:message      :varchar
                                       :created_at   :timeuuid
                                       :primary-key  [:message :created_at]}))
    (create-index s :events :created_at)
    (dotimes [i 10]
      (let [dt (date-time 2014 11 17 23 i)]
        (insert s :events {:created_at (uuids/start-of (cc/to-long dt))
                           :message    (format "Message %d" i)})))
    (let [xs  (select s :events
                      (where [[:in :message ["Message 5" "Message 6" "Message 7"
                                             "Message 8" "Message 9"]]
                              [>= :created_at (-> (date-time 2014 11 17 23 6)
                                                  .toDate
                                                  fns/min-timeuuid)]
                              [<= :created_at (-> (date-time 2014 11 17 23 8)
                                                  .toDate
                                                  fns/max-timeuuid)]]))]
      (is (= #{"Message 6" "Message 7" "Message 8"}
             (set (map :message xs)))))

    (drop-table s :events))


  (deftest test-paginate
    (create-table s :tv_series
                  (column-definitions {:series_title  :varchar
                                       :episode_id    :int
                                       :episode_title :text
                                       :primary-key [:series_title :episode_id]}))
    (dotimes [i 20]
      (insert s :tv_series {:series_title "Futurama" :episode_id i :episode_title (str "Futurama Title " i)})
      (insert s :tv_series {:series_title "Simpsons" :episode_id i :episode_title (str "Simpsons Title " i)}))


    (is (= (set (range 0 10))
           (->> (select s :tv_series
                        (paginate :key :episode_id :per-page 10 :where {:series_title "Futurama"}))
                (map :episode_id)
                set)))

    (is (= (set (range 11 20))
           (->> (select s :tv_series
                        (paginate :key :episode_id :per-page 10 :last-key 10 :where { :series_title "Futurama"}))
                (map :episode_id)
                set)))

    (drop-table s :tv_series))

  (deftest test-insert-with-consistency-level
    (let [r {:name "Alex" :city "Munich" :age (int 19)}]
      (client/execute s
                      "INSERT INTO users (name, city, age) VALUES ('Alex', 'Munich', 19);"
                      :consistency-level (cp/consistency-level :quorum))
      (is (= r (get-one s :users)))
      (truncate s :users)))

  (deftest test-insert-with-forced-prepared-statements
    (comment
      (let [r {:name "Alex" :city "Munich" :age (int 19)}]
        (cp/forcing-prepared-statements
         (insert s :users r))
        (is (= r (get-one s :users)))
        (truncate s :users))))

  (deftest test-insert-without-prepared-statements
    (comment
      (let [r {:name "Alex" :city "Munich" :age (int 19)}]
        (cp/without-prepared-statements
         (insert s :users r))
        (is (= r (get-one s :users)))
        (truncate s :users))))

  (deftest test-raw-cql-insert
    (testing "With default session"
      (client/execute s "INSERT INTO users (name, city, age) VALUES ('Alex', 'Munich', 19);")
      (is (= {:name "Alex" :city "Munich" :age (int 19)}
             (first (client/execute s "SELECT * FROM users;"))))
      (client/execute s "TRUNCATE users;"))
    (testing "Prepared statement"
      (client/execute s
                      (client/bind
                       (client/prepare s "INSERT INTO users (name, city, age) VALUES (?, ?, ?);")
                       ["Alex" "Munich" (int 19)]))
      (is (= {:name "Alex" :city "Munich" :age (int 19)}
             (first (client/execute s "SELECT * FROM users;"))))
      (client/execute s "TRUNCATE users;")))

  (deftest test-insert-nils
    (let [r {:name "Alex" :city "Munich" :age nil}]
      (client/execute s
                      (client/bind
                       (client/prepare s "INSERT INTO users (name, city, age) VALUES (?, ?, ?);")
                       ["Alex" "Munich" nil]))
      (is (= r (first (select s :users)))))))
