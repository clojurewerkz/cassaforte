(ns clojurewerkz.cassaforte.cql-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.policies :as cp]
            [clojurewerkz.cassaforte.cql :as cql :refer :all]
            [clojure.test :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]
            [qbits.hayt.dsl.statement :as hs]
            [qbits.hayt.dsl.clause :as hc]))

(let [s (client/connect ["127.0.0.1"])]
  (use-fixtures :each (fn [f]
                        (th/with-temporary-keyspace s f)))

  (deftest test-insert
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (insert s :users r)
       (is (= r (first (select s :users))))
       (truncate s :users))))

  (deftest test-insert-batch-with-ttl
    (th/test-combinations
     (let [input [[{:name "Alex" :city "Munich" :age (int 19)} (using :ttl (int 350))]
                  [{:name "Alex" :city "Munich" :age (int 19)} (using :ttl (int 350))]]]
       (insert-batch s :users input)
       (is (= (first (first input)) (first (select s :users))))
       (truncate s :users))))

  (deftest test-insert-batch-plain
    (th/test-combinations
     (let [input [{:name "Alex" :city "Munich" :age (int 19)}
                  {:name "Alex" :city "Munich" :age (int 19)}]]
       (insert-batch s :users input)
       (is (= (first input) (first (select s :users))))
       (truncate s :users))))

  (deftest test-update
    (testing "Simple updates"
      (th/test-combinations
       (let [r {:name "Alex" :city "Munich" :age (int 19)}]
         (insert s :users r)
         (is (= r (first (select s :users))))
         (update s :users
                 {:age (int 25)}
                 (where {:name "Alex"}))
         (is (= {:name "Alex" :city "Munich" :age (int 25)}
                (first (select s :users)))))))

    (testing "One of many update"
      (th/test-combinations
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

  (deftest ^:focus test-insert-with-atomic-batch
    (th/test-combinations
     (cql/atomic-batch s (queries
                          (hs/insert :users (values {:name "Alex" :city "Munich" :age (int 19)}))
                          (hs/insert :users (values {:name "Fritz" :city "Hamburg" :age (int 28)}))))
     (is (= "Munich" (-> (select s :users) first :city)))
     (truncate s :users)))

  (deftest test-delete
    (th/test-combinations
     (dotimes [i 3]
       (insert s :users {:name (str "name" i) :age (int i)}))
     (is (= 3 (perform-count s :users)))
     (delete s :users
             (where {:name "name1"}))
     (is (= 2 (perform-count s :users)))
     (truncate s :users))

    (th/test-combinations
     (insert s :users {:name "name1" :age (int 19)})
     (delete s :users
             (columns :age)
             (where {:name "name1"}))
     (is (nil? (:age (select s :users))))
     (truncate s :users)))

  (deftest test-insert-with-timestamp
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (insert s :users r
               (using :timestamp (.getTime (java.util.Date.))))
       (is (= r (first (select s :users))))
       (truncate s :users))))

  (deftest test-ttl
    (th/test-combinations
     (dotimes [i 3]
       (insert s :users {:name (str "name" i) :city (str "city" i) :age (int i)}
               (using :ttl (int 2))))
     (is (= 3 (perform-count s :users)))
     (Thread/sleep 2100)
     (is (= 0 (perform-count s :users)))))


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
    (th/test-combinations
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
     (truncate s :users)))

  (deftest test-index-filtering-range-alt-syntax
    (create-index s :users :city)
    (create-index s :users :age)
    (th/test-combinations
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
     (truncate s :users)))

  (deftest test-index-exact-match
    (create-index s :users :city)
    (th/test-combinations
     (dotimes [i 10]
       (insert s :users {:name (str "name_" i) :city (str "city_" i) :age (int i)}))


     (let [res (select s :users
                       (where {:city "city_5"})
                       (allow-filtering true))]
       (is (= 5
              (-> res
                  first
                  :age))))
     (truncate s :users)))

  (deftest test-list-operations
    (create-table s :users_list
                  (column-definitions
                   {:name :varchar
                    :test_list (list-type :varchar)
                    :primary-key [:name]}))

    (testing "Inserting"
      (th/test-combinations
       (insert s :users_list
               {:name "user1"
                :test_list ["str1" "str2" "str3"]})
       (is (= ["str1" "str2" "str3"] (get-in (select s :users_list)
                                             [0 :test_list])))
       (truncate s :users_list)))

    (testing "Updating"
      (th/test-combinations
       (insert s :users_list
               {:name "user1"
                :test_list []})
       (dotimes [i 3]
         (update s :users_list
                 {:test_list [+ [(str "str" i)]]}
                 (where {:name "user1"})))

       (is (= ["str0" "str1" "str2"] (get-in (select s :users_list)
                                             [0 :test_list])))
       (truncate s :users_list)))

    (testing "Deleting"
      (th/test-combinations
       (insert s :users_list
               {:name "user1"
                :test_list ["str0" "str1" "str2"]})
       (update s :users_list
               {:test_list [- ["str0" "str1"]]}
               (where {:name "user1"}))

       (is (= ["str2"] (get-in (select s :users_list)
                               [0 :test_list])))))

    (drop-table s :users_list))

  (deftest test-map-operations
    (create-table s :users_map
                  (column-definitions
                   {:name :varchar
                    :test_map (map-type :varchar :varchar)
                    :primary-key [:name]}))

    (testing "Inserting"
      (th/test-combinations
       (insert s :users_map
               {:name "user1"
                :test_map {"a" "b" "c" "d"}})
       (is (= {"a" "b" "c" "d"} (get-in (select s :users_map)
                                        [0 :test_map])))
       (truncate s :users_map)))

    (testing "Updating"
      (th/test-combinations
       (insert s :users_map
               {:name "user1"
                :test_map {}})
       (dotimes [i 3]
         (update s :users_map
                 {:test_map [+ {"a" "b" "c" "d"}]}
                 (where {:name "user1"})))

       (is (= {"a" "b" "c" "d"} (get-in (select s :users_map)
                                        [0 :test_map])))
       (truncate s :users_map)))
    (drop-table s :users_map))


  (deftest test-set-operations
    (create-table s :users_set
                  (column-definitions
                   {:name :varchar
                    :test_set (set-type :varchar)
                    :primary-key [:name]}))

    (testing "Inserting"
      (th/test-combinations
       (insert s :users_set
               {:name "user1"
                :test_set #{"str1" "str2" "str3"}})
       (is (= #{"str1" "str2" "str3"} (get-in (select s :users_set)
                                              [0 :test_set])))
       (truncate s :users_set)))


    (testing "Updating"
      (th/test-combinations
       (insert s :users_set
               {:name "user1"
                :test_set #{}})
       (dotimes [i 3]
         (dotimes [_ 2]
           (update s :users_set
                   {:test_set [+ #{(str "str" i)}]}
                   (where {:name "user1"}))))

       (is (= #{"str0" "str1" "str2"} (get-in (select s :users_set)
                                              [0 :test_set])))
       (truncate s :users_set)))

    (testing "Deleting"
      (th/test-combinations
       (insert s :users_set
               {:name "user1"
                :test_set #{"str0" "str1" "str2"}})
       (update s :users_set
               {:test_set [- #{"str0" "str1"}]}
               (where {:name "user1"}))

       (is (= #{"str2"} (get-in (select s :users_set)
                                [0 :test_set])))))

    (drop-table s :users_set))

  (deftest test-select-where
    (th/test-combinations
     (insert s :users {:name "Alex"   :city "Munich"        :age (int 19)})
     (insert s :users {:name "Robert" :city "Berlin"        :age (int 25)})
     (insert s :users {:name "Sam"    :city "San Francisco" :age (int 21)})

     (is (= "Munich" (get-in (select s :users (where {:name "Alex"})) [0 :city])))))

  (deftest test-select-in
    (th/test-combinations
     (insert s :users {:name "Alex"   :city "Munich"        :age (int 19)})
     (insert s :users {:name "Robert" :city "Berlin"        :age (int 25)})
     (insert s :users {:name "Sam"    :city "San Francisco" :age (int 21)})

     (let [users (select s :users
                         (where [[:in :name ["Alex" "Robert"]]]))]
       (is (= "Munich" (get-in users [0 :city])))
       (is (= "Berlin" (get-in users [1 :city]))))))

  (deftest test-select-order-by
    (th/test-combinations
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
                    (order-by [:post_id :desc]))))))

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
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (cp/with-consistency-level :quorum
         (insert s :users r))
       (is (= r (get-one s :users)))
       (truncate s :users))))

  (deftest test-insert-with-forced-prepared-statements
    (let [r {:name "Alex" :city "Munich" :age (int 19)}]
      (cp/forcing-prepared-statements
       (insert s :users r))
      (is (= r (get-one s :users)))
      (truncate s :users)))

  (deftest test-insert-without-prepared-statements
    (let [r {:name "Alex" :city "Munich" :age (int 19)}]
      (cp/without-prepared-statements
       (insert s :users r))
      (is (= r (get-one s :users)))
      (truncate s :users)))

  (deftest test-raw-cql-insert
    (testing "With default session"
      (client/execute s "INSERT INTO users (name, city, age) VALUES ('Alex', 'Munich', 19);")
      (is (= {:name "Alex" :city "Munich" :age (int 19)}
             (first (client/execute s "SELECT * FROM users;"))))
      (client/execute s "TRUNCATE users;"))
    (testing "Prepared statement"
      (client/execute s (client/as-prepared "INSERT INTO users (name, city, age) VALUES (?, ?, ?);"
                                            "Alex" "Munich" (int 19))
                      {:prepared true})
      (is (= {:name "Alex" :city "Munich" :age (int 19)}
             (first (client/execute s "SELECT * FROM users;"))))
      (client/execute s "TRUNCATE users;")))

  (deftest test-insert-nils
    (client/prepared
     (let [r {:name "Alex" :city "Munich" :age nil}]
       (insert s :users r)
       (is (= r (first (select s :users))))))))
