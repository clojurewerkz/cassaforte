(ns clojurewerkz.cassaforte.cql-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :as th :refer [*session* with-table]]
            [clojurewerkz.cassaforte.client      :as client]
            [clojurewerkz.cassaforte.policies    :as cp]
            [clojurewerkz.cassaforte.uuids       :as uuids]

            [clj-time.format                     :as tf]
            [clj-time.coerce                     :as cc]
            [clj-time.core                       :refer [seconds ago before? date-time] :as tc]

            [clojurewerkz.cassaforte.cql         :refer :all]

            [clojure.test                        :refer :all]
            [clojurewerkz.cassaforte.new-query-api :as new-query-api]
            [clojurewerkz.cassaforte.query.dsl :refer :all]
            ))


(use-fixtures :each th/with-temporary-keyspace)


(deftest test-insert
  (let [r {:name "Alex" :city "Munich" :age (int 19)}]
    (insert *session* :users r)
    (is (= r (first (select *session* :users))))
    (truncate *session* :users)))

(deftest test-update
  (testing "Simple updates"
    (let [r {:name "Alex"
             :city "Munich"
             :age  (int 19)}]
      (insert *session* :users r)
      (is (= r (first (select *session* :users))))
      (update *session* :users
              {:age (int 25)}
              (where {:name "Alex"}))
      (is (= {:name "Alex" :city "Munich" :age (int 25)}
             (first (select *session* :users))))))

  (testing "One of many update"
    (dotimes [i 3]
      (insert *session* :user_posts {:username "user1"
                                     :post_id (str "post" i)
                                     :body (str "body" i)}))

    (update *session* :user_posts
            {:body "bodynew"}
            (where {:username "user1"
                    :post_id "post1"}))
    (is (= "bodynew"
           (get-in
            (select *session* :user_posts
                    (where {:username "user1"
                            :post_id "post1"}))
            [0 :body])))))


(deftest test-update-with-compound-key
  (let [t   :events_by_device_id_and_date
        fmt (tf/formatter "yyyy-MM-dd")
        id  "device-000000001"
        qc  [[=  :device_id id]
             [=  :date "2014-11-13"]
             [=  :created_at (uuids/start-of (cc/to-long (date-time 2014 11 13 12)))]]]
    (testing "Bulk update"
      (truncate *session* t)
      (is (= 0 (perform-count *session* t)))
      (doseq [i  (range 1 15)]
        (let [dt (date-time 2014 11 i 12)
              td (uuids/start-of (cc/to-long dt))]

          (insert *session* t {:created_at td
                               :device_id  id
                               :date       (tf/unparse fmt dt)
                               :payload    (str "body" i)})))
      (is (= 14 (perform-count *session* t)))
      (is (= 14 (count (select *session* t))))
      (is (= 1  (count (select *session* t (where qc)))))
      (update *session* t
              {:payload "updated payload"}
              (where qc))
      (let [x (first (select *session* t (where qc)))]
        (is (= "updated payload" (:payload x))))
      (truncate *session* t))))

(deftest test-delete
  (testing "Delete whole row"
    (dotimes [i 3]
      (insert *session* :users {:name (str "name" i)
                                :age (int i)}))
    (is (= 3 (perform-count *session* :users)))
    (delete *session* :users
            (where {:name "name1"}))
    (is (= 2 (perform-count *session* :users)))
    (truncate *session* :users))

  (testing "Delete a column"
    (insert *session* :users {:name "name1" :age (int 19)})
    (delete *session* :users
            (columns :age)
            (where {:name "name1"}))
    (is (nil? (:age (select *session* :users))))
    (truncate *session* :users)))

(deftest test-insert-with-timestamp
  (let [r {:name "Alex" :city "Munich" :age (int 19)}]
    (insert *session* :users
            r
            (using {:timestamp (.getTime (java.util.Date.))}))
    (is (= r (first (select *session* :users))))
    (truncate *session* :users)))

(deftest test-ttl
  (dotimes [i 3]
    (insert *session* :users {:name (str "name" i)
                              :city (str "city" i)
                              :age  (int i)}
            (using {:ttl (int 2)})))
  (is (= 3 (perform-count *session* :users)))
  (Thread/sleep 2100)
  (is (= 0 (perform-count *session* :users))))


(deftest test-counter
  (testing "Increment by"
    (update *session* :user_counters
            {:user_count (new-query-api/increment-by 5)}
            (where {:name "user1"}))

    (is (= 5 (-> (select *session* :user_counters)
                 first
                 :user_count))))

  (testing "Decrement by"
    (update *session* :user_counters
            {:user_count (new-query-api/decrement-by 5)}
            (where {:name "user1"}))

    (is (= 0 (-> (select *session* :user_counters)
                 first
                 :user_count))))

  (testing "Increment with explicit params"
    (update *session* :user_counters
            {:user_count (new-query-api/increment-by 500)}
            (where {:name "user1"}))
    (is (= 500 (-> (select *session* :user_counters)
                   first
                   :user_count))))

  (testing "Decrement with explicit params"
    (update *session* :user_counters
            {:user_count (new-query-api/increment-by 5000000)}
            (where {:name "user1"}))
    (is (= 5000500 (-> (select *session* :user_counters)
                       first
                       :user_count))))

  (testing "Increment with a large number"
    (update *session* :user_counters
            {:user_count (new-query-api/increment-by 50000000000000)}
            (where {:name "user1"}))
    (is (= 50000005000500 (-> *session*
                              (select :user_counters)
                              first
                              :user_count)))))

(deftest test-counter-2
  (dotimes [i 100]
    (update *session* :user_counters
            {:user_count (new-query-api/increment)}
            (where {:name "user1"})))
  (is (= 100 (-> (select *session* :user_counters)
                 first
                 :user_count))))

(deftest test-index-filtering-range
  (create-index *session* :city
                (new-query-api/on-table :users)
                (new-query-api/and-column :city))
  (create-index *session* :age
                (new-query-api/on-table :users)
                (new-query-api/and-column :age))

  (dotimes [i 10]
    (insert *session* :users {:name (str "name_" i)
                              :city "Munich"
                              :age  (int i)}))

  (is (= (set (range 6 10))
         (->> (select *session* :users
                      (where [[= :city "Munich"]
                              [> :age (int 5)]])
                      (allow-filtering))
              (map :age)
              set))))

(deftest test-index-filtering-range-alt-syntax
  (create-index *session* :city
                (new-query-api/on-table :users)
                (new-query-api/and-column :city))
  (create-index *session* :age
                (new-query-api/on-table :users)
                (new-query-api/and-column :age))

  (dotimes [i 10]
    (insert *session* :users {:name (str "name_" i)
                              :city "Munich"
                              :age  (int i)}))

  (let [res (select *session* :users
                    (where [[= :city "Munich"]
                            [> :age (int 5)]])
                    (allow-filtering))]
    (is (= (set (range 6 10))
           (->> res
                (map :age)
                set))))
  (truncate *session* :users))

(deftest test-index-exact-match
  (create-index *session* :city
                (new-query-api/on-table :users)
                (new-query-api/and-column :city))
  (create-index *session* :age
                (new-query-api/on-table :users)
                (new-query-api/and-column :age))

  (dotimes [i 10]
    (insert *session* :users {:name (str "name_" i)
                              :city (str "city_" i)
                              :age  (int i)}))

  (let [res (select *session* :users
                    (where {:city "city_5"})
                    (allow-filtering))]
    (is (= 5
           (-> res
               first
               :age))))
  (truncate *session* :users))

(deftest test-select-where
  (insert *session* :users {:name "Alex"
                            :city "Munich"
                            :age  (int 19)})
  (insert *session* :users {:name "Robert"
                            :city "Berlin"
                            :age  (int 25)})
  (insert *session* :users {:name "Sam"
                            :city "San Francisco"
                            :age  (int 21)})

  (is (= "Munich" (get-in (select *session* :users (where {:name "Alex"})) [0 :city]))))

(deftest test-select-where-without-fetch
  (insert *session* :users {:name "Alex"
                            :city "Munich"
                            :age (int 19)})
  (insert *session* :users {:name "Robert"
                            :city "Berlin"
                            :age  (int 25)})
  (insert *session* :users {:name "Sam"
                            :city "San Francisco"
                            :age  (int 21)})

  (is (= "Munich" (get-in (client/execute *session*
                                          "SELECT * FROM users where name='Alex';"
                                          :fetch-size Integer/MAX_VALUE) [0 :city]))))

(deftest test-select-in
  (insert *session* :users {:name "Alex"
                            :city "Munich"
                            :age  (int 19)})
  (insert *session* :users {:name "Robert"
                            :city "Berlin"
                            :age  (int 25)})
  (insert *session* :users {:name "Sam"
                            :city "San Francisco"
                            :age  (int 21)})

  (let [users (select *session* :users
                      (where [[:in :name ["Alex" "Robert"]]]))]
    (is (= "Munich" (get-in users [0 :city])))
    (is (= "Berlin" (get-in users [1 :city])))))

(deftest test-select-order-by
  (dotimes [i 3]
    (insert *session* :user_posts
            {:username "Alex"
             :post_id  (str "post" i)
             :body     (str "body" i)}))

  (is (= [{:post_id "post0"}
          {:post_id "post1"}
          {:post_id "post2"}]
         (select *session* :user_posts
                 (columns :post_id)
                 (where {:username "Alex"})
                 (order-by :post_id))))

  (is (= [{:post_id "post2"}
          {:post_id "post1"}
          {:post_id "post0"}]
         (select *session* :user_posts
                 (columns :post_id)
                 (where {:username "Alex"})
                 (order-by (new-query-api/desc :post_id))))))

(deftest test-timeuuid-now-and-unix-timestamp-of
  (let [dt (-> 2 seconds ago)
        ts (cc/to-long dt)]
    (dotimes [i 20]
      (insert *session* :events
              {:created_at (new-query-api/now)
               :message    (format "Message %d" i)}))
    (let [xs  (select *session* :events
                      (new-query-api/unix-timestamp-of :created_at)
                      (limit 5))
          ts' (get (first xs) (keyword "unixTimestampOf(created_at)"))]
      (is (> ts' ts)))))

(deftest test-timeuuid-dateof
  (let [dt (-> 2 seconds ago)]
    (dotimes [i 20]
      (insert *session* :events
              {:created_at (new-query-api/now)
               :message (format "Message %d" i)}))
    (let [xs  (select *session* :events
                      (new-query-api/date-of :created_at)
                      (limit 5))
          dt' (cc/from-date (get (first xs) (keyword "dateOf(created_at)")))]
      (is (before? dt dt')))))

(deftest test-timeuuid-min-open-range-query
  (dotimes [i 10]
    (let [dt (date-time 2014 11 17 23 i)]
      (insert *session* :events_for_in_and_range_query
              {:created_at (uuids/start-of (cc/to-long dt))
               :city       "London, UK"
               :message    (format "Message %d" i)})))
  (let [xs  (select *session* :events_for_in_and_range_query
                    (where [[:in :message ["Message 7" "Message 8" "Message 9" "Message 10"]]
                            [>= :created_at (-> (date-time 2014 11 17 23 8)
                                                .toDate
                                                new-query-api/min-timeuuid)]]))]
    (is (= #{"Message 8" "Message 9"}
           (set (map :message xs))))))

(deftest test-timeuuid-min-max-timeuuid-range-query
  (dotimes [i 10]
    (let [dt (date-time 2014 11 17 23 i)]
      (insert *session* :events_for_in_and_range_query
              {:created_at (uuids/start-of (cc/to-long dt))
               :message    (format "Message %d" i)})))
  (let [xs  (select *session* :events_for_in_and_range_query
                    (where [[:in :message ["Message 5" "Message 6" "Message 7"
                                           "Message 8" "Message 9"]]
                            [>= :created_at (-> (date-time 2014 11 17 23 6)
                                                .toDate
                                                new-query-api/min-timeuuid)]
                            [<= :created_at (-> (date-time 2014 11 17 23 8)
                                                .toDate
                                                new-query-api/max-timeuuid)]]))]
    (is (= #{"Message 6" "Message 7" "Message 8"}
           (set (map :message xs))))))


(deftest test-select-range-query
  (create-table *session* :tv_series
                (column-definitions {:series_title  :varchar
                                     :episode_id    :int
                                     :episode_title :text
                                     :primary-key   [:series_title :episode_id]}))
  (dotimes [i 20]
    (insert *session* :tv_series
            {:series_title  "Futurama"
             :episode_id    i
             :episode_title (str "Futurama Title " i)})
    (insert *session* :tv_series
            {:series_title  "Simpsons"
             :episode_id    i
             :episode_title (str "Simpsons Title " i)}))

  (is (= (set (range 11 20))
         (->> (select *session* :tv_series
                      (where [[= :series_title "Futurama"]
                              [> :episode_id 10]]))
              (map :episode_id )
              set)))

  (is (= (set (range 11 16))
         (->> (select *session* :tv_series
                      (where [[=  :series_title "Futurama"]
                              [>  :episode_id 10]
                              [<= :episode_id 15]]))
              (map :episode_id)
              set)))

  (is (= (set (range 0 15))
         (->> (select *session* :tv_series
                      (where [[= :series_title "Futurama"]
                              [< :episode_id 15]]))
              (map :episode_id)
              set)))

  (drop-table *session* :tv_series))

(deftest test-paginate
  (with-table :tv_series
    (dotimes [i 20]
      (insert *session* :tv_series
              {:series_title  "Futurama"
               :episode_id    i
               :episode_title (str "Futurama Title " i)})
      (insert *session* :tv_series
              {:series_title  "Simpsons"
               :episode_id    i
               :episode_title (str "Simpsons Title " i)}))


    (is (= (set (range 0 10))
           (->> (select *session* :tv_series
                        (paginate :key :episode_id :per-page 10
                                  :where {:series_title "Futurama"}))
                (map :episode_id)
                set)))

    (is (= (set (range 11 20))
           (->> (select *session* :tv_series
                        (paginate :key :episode_id :per-page 10 :last-key 10
                                  :where {:series_title "Futurama"}))
                (map :episode_id)
                set)))))

(deftest test-insert-with-consistency-level
  (let [r {:name "Alex" :city "Munich" :age (int 19)}]
    (client/execute *session*
                    "INSERT INTO users (name, city, age) VALUES ('Alex', 'Munich', 19);"
                    :consistency-level (cp/consistency-level :quorum))
    (is (= r (first (select *session* :users (limit 1)))))
    (truncate *session* :users)))

(deftest test-insert-with-forced-prepared-statements
  (let [r        {:name "Alex" :city "Munich" :age (int 19)}
        prepared (client/prepare *session*
                  (new-query-api/insert :users
                                        {:name new-query-api/?
                                         :city new-query-api/?
                                         :age  new-query-api/?}))]
    (client/execute *session*
                    (client/bind prepared
                                 {:name "Alex" :city "Munich" :age (int 19)}))
    (is (= r (first (select *session* :users (limit 1)))))))

(deftest test-raw-cql-insert
  (testing "With default session"
    (client/execute *session* "INSERT INTO users (name, city, age) VALUES ('Alex', 'Munich', 19);")
    (is (= {:name "Alex" :city "Munich" :age (int 19)}
           (first (client/execute *session* "SELECT * FROM users;"))))
    (client/execute *session* "TRUNCATE users;"))
  (testing "Prepared statement"
    (client/execute *session*
                    (client/bind
                     (client/prepare *session* "INSERT INTO users (name, city, age) VALUES (?, ?, ?);")
                     ["Alex" "Munich" (int 19)]))
    (is (= {:name "Alex" :city "Munich" :age (int 19)}
           (first (client/execute *session* "SELECT * FROM users;"))))
    (client/execute *session* "TRUNCATE users;")))

(deftest test-insert-nils
  (let [r {:name "Alex" :city "Munich" :age nil}]
    (client/execute *session*
                    (client/bind
                     (client/prepare *session* "INSERT INTO users (name, city, age) VALUES (?, ?, ?);")
                     ["Alex" "Munich" nil]))
    (is (= r (first (select *session* :users))))))
