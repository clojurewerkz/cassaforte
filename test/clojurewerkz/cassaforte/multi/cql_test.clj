(ns clojurewerkz.cassaforte.multi.cql-test
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.client :as client])
  (:use clojurewerkz.cassaforte.multi.cql
        clojure.test
        clojurewerkz.cassaforte.query))

(use-fixtures :each th/initialize!)

(deftest insert-test
  (th/test-combinations
   (let [r {:name "Alex" :city "Munich" :age (int 19)}]
     (insert th/session :users r)
     (is (= r (first (select th/session :users))))
     (truncate th/session :users))))

(deftest update-test
  (testing "Simple updates"
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (insert th/session :users r)
       (is (= r (first (select th/session :users))))
       (update th/session :users
               {:age (int 25)}
               (where :name "Alex"))
       (is (= {:name "Alex" :city "Munich" :age (int 25)}
              (first (select th/session :users)))))))

  (testing "One of many update"
    (th/test-combinations
     (dotimes [i 3]
       (insert th/session :user_posts {:username "user1"
                                       :post_id (str "post" i)
                                       :body (str "body" i)}))
     (update th/session :user_posts
             {:body "bodynew"}
             (where :username "user1"
                    :post_id "post1"))
     (is (= "bodynew"
            (get-in
             (select th/session :user_posts
                     (where :username "user1"
                            :post_id "post1"))
             [0 :body]))))))

(deftest delete-test
  (th/test-combinations
   (dotimes [i 3]
     (insert th/session :users {:name (str "name" i) :age (int i)}))
   (is (= 3 (perform-count th/session :users)))
   (delete th/session :users
           (where :name "name1"))
   (is (= 2 (perform-count th/session :users)))
   (truncate th/session :users))

  (th/test-combinations
   (insert th/session :users {:name "name1" :age (int 19)})
   (delete th/session :users
           (columns :age)
           (where :name "name1"))
   (is (nil? (:age (select th/session :users))))
   (truncate th/session :users)))

(deftest insert-with-timestamp-test
  (th/test-combinations
   (let [r {:name "Alex" :city "Munich" :age (int 19)}]
     (insert th/session :users r
             (using :timestamp (.getTime (java.util.Date.))))
     (is (= r (first (select th/session :users))))
     (truncate th/session :users))))

(deftest ttl-test
  (th/test-combinations
   (dotimes [i 3]
     (insert th/session :users {:name (str "name" i) :city (str "city" i) :age (int i)}
             (using :ttl 2)))
   (is (= 3 (perform-count th/session :users)))
   (Thread/sleep 2100)
   (is (= 0 (perform-count th/session :users)))))

(deftest counter-test
  (update th/session :user_counters {:user_count [+ 5]} (where :name "asd"))
  (is (= 5 (:user_count (first (select th/session :user_counters)))))
  (update th/session :user_counters {:user_count [+ 500]} (where :name "asd"))
  (is (= 505 (:user_count (first (select th/session :user_counters)))))
  (update th/session :user_counters {:user_count [+ 5000000]} (where :name "asd"))
  (is (= 5000505 (:user_count (first (select th/session :user_counters)))))
  (update th/session :user_counters {:user_count [+ 50000000000000]} (where :name "asd"))
  (is (= 50000005000505 (:user_count (first (select th/session :user_counters))))))

(deftest counter-test-2
  (dotimes [i 100]
    (update th/session :user_counters {:user_count [+ 1]} (where :name "asd")))
  (is (= 100 (:user_count (first (select th/session :user_counters))))))

(deftest index-test-filtering-range
  (create-index th/session :users :city)
  (create-index th/session :users :age)
  (th/test-combinations
   (dotimes [i 10]
     (insert th/session :users {:name (str "name_" i) :city "Munich" :age (int i)}))

   (let [res (select th/session :users
                     (where :city "Munich"
                            :age [> (int 5)])
                     (allow-filtering true))]
     (is (= (set (range 6 10))
            (->> res
                 (map :age)
                 set))))
   (truncate th/session :users)))

(deftest index-exact-match
  (create-index th/session :users :city)
  (th/test-combinations
   (dotimes [i 10]
     (insert th/session :users {:name (str "name_" i) :city (str "city_" i) :age (int i)}))


   (let [res (select th/session :users
                     (where :city "city_5")
                     (allow-filtering true))]
     (is (= 5
            (->> res
                 first
                 :age))))
   (truncate th/session :users)))

(deftest list-operations-test
  (create-table th/session
                :users_list
                (column-definitions
                 {:name :varchar
                  :test_list (list-type :varchar)
                  :primary-key [:name]}))

  (testing "Inserting"
    (th/test-combinations
     (insert th/session :users_list
             {:name "user1"
              :test_list ["str1" "str2" "str3"]})
     (is (= ["str1" "str2" "str3"] (get-in (select th/session :users_list)
                                           [0 :test_list])))
     (truncate th/session :users_list)))

  (testing "Updating"
    (th/test-combinations
     (insert th/session :users_list
             {:name "user1"
              :test_list []})
     (dotimes [i 3]
       (update th/session :users_list
               {:test_list [+ [(str "str" i)]]}
               (where :name "user1")))

     (is (= ["str0" "str1" "str2"] (get-in (select th/session :users_list)
                                           [0 :test_list])))
     (truncate th/session :users_list)))

  (testing "Deleting"
    (th/test-combinations
     (insert th/session :users_list
             {:name "user1"
              :test_list ["str0" "str1" "str2"]})
     (update th/session :users_list
             {:test_list [- ["str0" "str1"]]}
             (where :name "user1"))

     (is (= ["str2"] (get-in (select th/session :users_list)
                             [0 :test_list])))))

  (drop-table th/session :users_list))

(deftest map-operations-test
  (create-table th/session :users_map
                (column-definitions
                 {:name :varchar
                  :test_map (map-type :varchar :varchar)
                  :primary-key [:name]}))

  (testing "Inserting"
    (th/test-combinations
     (insert th/session :users_map
             {:name "user1"
              :test_map {"a" "b" "c" "d"}})
     (is (= {"a" "b" "c" "d"} (get-in (select th/session :users_map)
                                      [0 :test_map])))
     (truncate th/session :users_map)))

  (testing "Updating"
    (th/test-combinations
     (insert th/session :users_map
             {:name "user1"
              :test_map {}})
     (dotimes [i 3]
       (update th/session :users_map
               {:test_map [+ {"a" "b" "c" "d"}]}
               (where :name "user1")))

     (is (= {"a" "b" "c" "d"} (get-in (select th/session :users_map)
                                      [0 :test_map])))
     (truncate th/session :users_map)))
  (drop-table th/session :users_map))

(deftest set-operations-test
  (create-table th/session :users_set
                (column-definitions
                 {:name :varchar
                  :test_set (set-type :varchar)
                  :primary-key [:name]}))

  (testing "Inserting"
    (th/test-combinations
     (insert th/session :users_set
             {:name "user1"
              :test_set #{"str1" "str2" "str3"}})
     (is (= #{"str1" "str2" "str3"} (get-in (select th/session :users_set)
                                            [0 :test_set])))
     (truncate th/session :users_set)))


  (testing "Updating"
    (th/test-combinations
     (insert th/session :users_set
             {:name "user1"
              :test_set #{}})
     (dotimes [i 3]
       (dotimes [_ 2]
         (update th/session :users_set
                 {:test_set [+ #{(str "str" i)}]}
                 (where :name "user1"))))

     (is (= #{"str0" "str1" "str2"} (get-in (select th/session :users_set)
                                            [0 :test_set])))
     (truncate th/session :users_set)))

  (testing "Deleting"
    (th/test-combinations
     (insert th/session :users_set
             {:name "user1"
              :test_set #{"str0" "str1" "str2"}})
     (update th/session :users_set
             {:test_set [- #{"str0" "str1"}]}
             (where :name "user1"))

     (is (= #{"str2"} (get-in (select th/session :users_set)
                              [0 :test_set])))))

  (drop-table th/session :users_set))

(deftest select-where-test
  (th/test-combinations
   (doto th/session
     (insert :users {:name "Alex"   :city "Munich"        :age (int 19)})
     (insert :users {:name "Robert" :city "Berlin"        :age (int 25)})
     (insert :users {:name "Sam"    :city "San Francisco" :age (int 21)}))

   (is (= "Munich" (get-in (select th/session :users
                                   (where :name "Alex"))
                           [0 :city])))))

(deftest select-in-test
  (th/test-combinations
   (doto th/session
     (insert :users {:name "Alex"   :city "Munich"        :age (int 19)})
     (insert :users {:name "Robert" :city "Berlin"        :age (int 25)})
     (insert :users {:name "Sam"    :city "San Francisco" :age (int 21)}))

   (let [users (select th/session :users
                       (where :name [:in ["Alex" "Robert"]]))]
     (is (= "Munich" (get-in users [0 :city])))
     (is (= "Berlin" (get-in users [1 :city]))))))

(deftest select-order-by-test
  (th/test-combinations
   (dotimes [i 3]
     (insert th/session :user_posts {:username "Alex" :post_id  (str "post" i) :body (str "body" i)}))

   (is (= [{:post_id "post0"}
           {:post_id "post1"}
           {:post_id "post2"}]
          (select th/session :user_posts
                  (columns :post_id)
                  (where :username "Alex")
                  (order-by [:post_id]))))

   (is (= [{:post_id "post2"}
           {:post_id "post1"}
           {:post_id "post0"}]
          (select th/session :user_posts
                  (columns :post_id)
                  (where :username "Alex")
                  (order-by [:post_id :desc]))))))

(deftest select-range-query-test
  (create-table th/session :tv_series
                (column-definitions {:series_title  :varchar
                                     :episode_id    :int
                                     :episode_title :text
                                     :primary-key [:series_title :episode_id]}))
  (dotimes [i 20]
    (doto th/session
      (insert :tv_series {:series_title "Futurama" :episode_id i :episode_title (str "Futurama Title " i)})
      (insert :tv_series {:series_title "Simpsons" :episode_id i :episode_title (str "Simpsons Title " i)})))

  (is (= (set (range 11 20))
         (->> (select th/session :tv_series
                      (where :series_title "Futurama"
                             :episode_id [> 10]))
              (map :episode_id )
              set)))

  (is (= (set (range 11 16))
         (->> (select th/session :tv_series
                      (where :series_title "Futurama"
                             :episode_id [> 10]
                             :episode_id [<= 15]))
              (map :episode_id)
              set)))

  (is (= (set (range 0 15))
         (->> (select th/session :tv_series
                      (where :series_title "Futurama"
                             :episode_id [< 15]))
              (map :episode_id)
              set)))

  (drop-table th/session :tv_series))

(deftest paginate-test
  (create-table th/session :tv_series
                (column-definitions {:series_title  :varchar
                                     :episode_id    :int
                                     :episode_title :text
                                     :primary-key [:series_title :episode_id]}))
  (dotimes [i 20]
    (doto th/session
      (insert :tv_series {:series_title "Futurama" :episode_id i :episode_title (str "Futurama Title " i)})
      (insert :tv_series {:series_title "Simpsons" :episode_id i :episode_title (str "Simpsons Title " i)})))

  (is (= (set (range 0 10))
         (->> (select th/session :tv_series
                      (paginate :key :episode_id :per-page 10 :where { :series_title "Futurama"}))
              (map :episode_id)
              set)))

  (is (= (set (range 11 20))
         (->> (select th/session :tv_series
                      (paginate :key :episode_id :per-page 10 :last-key 10 :where { :series_title "Futurama"}))
              (map :episode_id)
              set)))

  (drop-table th/session :tv_series))

(deftest insert-with-consistency-level-test
  (testing "New DSL"
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (client/with-consistency-level :quorum
         (insert th/session :users r))
       (is (= r (first (select th/session :users))))
       (truncate th/session :users))))
  (testing "Old DSL"
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (client/with-consistency-level (client/consistency-level :quorum)
         (insert th/session :users r))
       (is (= r (first (select th/session :users))))
       (truncate th/session :users)))))

(deftest multi-connect
  (let [session (-> (client/build-cluster {:contact-points ["127.0.0.1"]
                                           :port 19042})
                    (client/connect :new_cql_keyspace))]
    (let [r {:name "Alex" :city "Munich" :age (int 19)}]
      (insert session :users r)
      (is (= r (first (select session :users))))
      (truncate session :users))))

(deftest insert-test-raw
  (testing "With default session"
    (client/execute th/session "INSERT INTO users (name, city, age) VALUES ('Alex', 'Munich', 19);")
    (is (= {:name "Alex" :city "Munich" :age (int 19)}
           (first (client/execute th/session "SELECT * FROM users;"))))
    (client/execute th/session "TRUNCATE users;")))
