(ns clojurewerkz.cassaforte.cql-test
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojure.test :refer :all]
            [clojurewerkz.cassaforte.query :refer :all]))

(use-fixtures :each th/initialize!)

(deftest test-insert
  (th/test-combinations
   (let [r {:name "Alex" :city "Munich" :age (int 19)}]
     (insert :users r)
     (is (= r (first (select :users))))
     (truncate :users))))

(deftest test-insert-batch-with-ttl
  (th/test-combinations
   (let [input [[{:name "Alex" :city "Munich" :age (int 19)} (using :ttl 350)]
                [{:name "Alex" :city "Munich" :age (int 19)} (using :ttl 350)]]]
     (insert-batch :users input)
     (is (= (first (first input)) (first (select :users))))
     (truncate :users))))

(deftest test-insert-batch-plain
  (th/test-combinations
   (let [input [{:name "Alex" :city "Munich" :age (int 19)}
                {:name "Alex" :city "Munich" :age (int 19)}]]
     (insert-batch :users input)
     (is (= (first input) (first (select :users))))
     (truncate :users))))

(deftest test-update
  (testing "Simple updates"
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (insert :users r)
       (is (= r (first (select :users))))
       (update :users
               {:age (int 25)}
               (where :name "Alex"))
       (is (= {:name "Alex" :city "Munich" :age (int 25)}
              (first (select :users)))))))

  (testing "One of many update"
    (th/test-combinations
     (dotimes [i 3]
       (insert :user_posts {:username "user1"
                            :post_id (str "post" i)
                            :body (str "body" i)}))
     (update :user_posts
             {:body "bodynew"}
             (where :username "user1"
                    :post_id "post1"))
     (is (= "bodynew"
            (get-in
             (select :user_posts
                     (where :username "user1"
                            :post_id "post1"))
             [0 :body]))))))

(deftest test-delete
  (th/test-combinations
   (dotimes [i 3]
     (insert :users {:name (str "name" i) :age (int i)}))
   (is (= 3 (perform-count :users)))
   (delete :users
           (where :name "name1"))
   (is (= 2 (perform-count :users)))
   (truncate :users))

  (th/test-combinations
   (insert :users {:name "name1" :age (int 19)})
   (delete :users
           (columns :age)
           (where :name "name1"))
   (is (nil? (:age (select :users))))
   (truncate :users)))

(deftest test-insert-with-timestamp
  (th/test-combinations
   (let [r {:name "Alex" :city "Munich" :age (int 19)}]
     (insert :users r
             (using :timestamp (.getTime (java.util.Date.))))
     (is (= r (first (select :users))))
     (truncate :users))))

(deftest testlttl
  (th/test-combinations
   (dotimes [i 3]
     (insert :users {:name (str "name" i) :city (str "city" i) :age (int i)}
             (using :ttl 2)))
   (is (= 3 (perform-count :users)))
   (Thread/sleep 2100)
   (is (= 0 (perform-count :users)))))


(deftest test-counter
  (update :user_counters {:user_count (increment-by 5)} (where :name "user1"))
  (is (= 5 (:user_count (first (select :user_counters)))))
  (update :user_counters {:user_count (decrement-by 5)} (where :name "user1"))
  (is (= 0 (:user_count (first (select :user_counters)))))
  (update :user_counters {:user_count [+ 500]} (where :name "user1"))
  (is (= 500 (:user_count (first (select :user_counters)))))
  (update :user_counters {:user_count [+ 5000000]} (where :name "user1"))
  (is (= 5000500 (:user_count (first (select :user_counters)))))
  (update :user_counters {:user_count [+ 50000000000000]} (where :name "user1"))
  (is (= 50000005000500 (:user_count (first (select :user_counters))))))

(deftest test-counter-2
  (dotimes [i 100]
    (update :user_counters {:user_count [+ 1]} (where :name "user1")))
  (is (= 100 (:user_count (first (select :user_counters))))))

(deftest test-index-filtering-range
  (create-index :users :city)
  (create-index :users :age)
  (th/test-combinations

   (dotimes [i 10]
     (insert :users {:name (str "name_" i) :city "Munich" :age (int i)}))

   (let [res (select :users
                     (where :city "Munich"
                            :age [> (int 5)])
                     (allow-filtering true))]
     (is (= (set (range 6 10))
            (->> res
                 (map :age)
                 set))))
   (truncate :users)))

(deftest test-index-filtering-range-alt-syntax
  (create-index :users :city)
  (create-index :users :age)
  (th/test-combinations

   (dotimes [i 10]
     (insert :users {:name (str "name_" i) :city "Munich" :age (int i)}))

   (let [res (select :users
                     (where {:city "Munich"
                             :age [> (int 5)]})
                     (allow-filtering true))]
     (is (= (set (range 6 10))
            (->> res
                 (map :age)
                 set))))
   (truncate :users)))

(deftest test-index-exact-match
  (create-index :users :city)
  (th/test-combinations
   (dotimes [i 10]
     (insert :users {:name (str "name_" i) :city (str "city_" i) :age (int i)}))


   (let [res (select :users
                     (where :city "city_5")
                     (allow-filtering true))]
     (is (= 5
            (->> res
                 first
                 :age))))
   (truncate :users)))

(deftest test-list-operations
  (create-table :users_list
                (column-definitions
                 {:name :varchar
                  :test_list (list-type :varchar)
                  :primary-key [:name]}))

  (testing "Inserting"
    (th/test-combinations
     (insert :users_list
             {:name "user1"
              :test_list ["str1" "str2" "str3"]})
     (is (= ["str1" "str2" "str3"] (get-in (select :users_list)
                                           [0 :test_list])))
     (truncate :users_list)))

  (testing "Updating"
    (th/test-combinations
     (insert :users_list
             {:name "user1"
              :test_list []})
     (dotimes [i 3]
       (update :users_list
               {:test_list [+ [(str "str" i)]]}
               (where :name "user1")))

     (is (= ["str0" "str1" "str2"] (get-in (select :users_list)
                                           [0 :test_list])))
     (truncate :users_list)))

  (testing "Deleting"
    (th/test-combinations
     (insert :users_list
             {:name "user1"
              :test_list ["str0" "str1" "str2"]})
     (update :users_list
             {:test_list [- ["str0" "str1"]]}
             (where :name "user1"))

     (is (= ["str2"] (get-in (select :users_list)
                             [0 :test_list])))))

  (drop-table :users_list))

(deftest test-map-operations
  (create-table :users_map
                (column-definitions
                 {:name :varchar
                  :test_map (map-type :varchar :varchar)
                  :primary-key [:name]}))

  (testing "Inserting"
    (th/test-combinations
     (insert :users_map
             {:name "user1"
              :test_map {"a" "b" "c" "d"}})
     (is (= {"a" "b" "c" "d"} (get-in (select :users_map)
                                      [0 :test_map])))
     (truncate :users_map)))

  (testing "Updating"
    (th/test-combinations
     (insert :users_map
             {:name "user1"
              :test_map {}})
     (dotimes [i 3]
       (update :users_map
               {:test_map [+ {"a" "b" "c" "d"}]}
               (where :name "user1")))

     (is (= {"a" "b" "c" "d"} (get-in (select :users_map)
                                      [0 :test_map])))
     (truncate :users_map)))
  (drop-table :users_map))


(deftest test-set-operations
  (create-table :users_set
                (column-definitions
                 {:name :varchar
                  :test_set (set-type :varchar)
                  :primary-key [:name]}))

  (testing "Inserting"
    (th/test-combinations
     (insert :users_set
             {:name "user1"
              :test_set #{"str1" "str2" "str3"}})
     (is (= #{"str1" "str2" "str3"} (get-in (select :users_set)
                                            [0 :test_set])))
     (truncate :users_set)))


  (testing "Updating"
    (th/test-combinations
     (insert :users_set
             {:name "user1"
              :test_set #{}})
     (dotimes [i 3]
       (dotimes [_ 2]
         (update :users_set
                 {:test_set [+ #{(str "str" i)}]}
                 (where :name "user1"))))

     (is (= #{"str0" "str1" "str2"} (get-in (select :users_set)
                                            [0 :test_set])))
     (truncate :users_set)))

  (testing "Deleting"
    (th/test-combinations
     (insert :users_set
             {:name "user1"
              :test_set #{"str0" "str1" "str2"}})
     (update :users_set
             {:test_set [- #{"str0" "str1"}]}
             (where :name "user1"))

     (is (= #{"str2"} (get-in (select :users_set)
                              [0 :test_set])))))

  (drop-table :users_set))

(deftest test-select-where
  (th/test-combinations
   (insert :users {:name "Alex"   :city "Munich"        :age (int 19)})
   (insert :users {:name "Robert" :city "Berlin"        :age (int 25)})
   (insert :users {:name "Sam"    :city "San Francisco" :age (int 21)})

   (is (= "Munich" (get-in (select :users (where :name "Alex")) [0 :city])))))

(deftest test-select-in
  (th/test-combinations
   (insert :users {:name "Alex"   :city "Munich"        :age (int 19)})
   (insert :users {:name "Robert" :city "Berlin"        :age (int 25)})
   (insert :users {:name "Sam"    :city "San Francisco" :age (int 21)})

   (let [users (select :users
                       (where :name [:in ["Alex" "Robert"]]))]
     (is (= "Munich" (get-in users [0 :city])))
     (is (= "Berlin" (get-in users [1 :city]))))))

(deftest test-select-order-by
  (th/test-combinations
   (dotimes [i 3]
     (insert :user_posts {:username "Alex" :post_id  (str "post" i) :body (str "body" i)}))

   (is (= [{:post_id "post0"}
           {:post_id "post1"}
           {:post_id "post2"}]
          (select :user_posts
                  (columns :post_id)
                  (where :username "Alex")
                  (order-by [:post_id]))))

   (is (= [{:post_id "post2"}
           {:post_id "post1"}
           {:post_id "post0"}]
          (select :user_posts
                  (columns :post_id)
                  (where :username "Alex")
                  (order-by [:post_id :desc]))))))

(deftest test-select-range-query
  (create-table :tv_series
                (column-definitions {:series_title  :varchar
                                     :episode_id    :int
                                     :episode_title :text
                                     :primary-key [:series_title :episode_id]}))
  (dotimes [i 20]
    (insert :tv_series {:series_title "Futurama" :episode_id i :episode_title (str "Futurama Title " i)})
    (insert :tv_series {:series_title "Simpsons" :episode_id i :episode_title (str "Simpsons Title " i)}))

  (is (= (set (range 11 20))
         (->> (select :tv_series
                      (where :series_title "Futurama"
                             :episode_id [> 10]))
              (map :episode_id )
              set)))

  (is (= (set (range 11 16))
         (->> (select :tv_series
                      (where :series_title "Futurama"
                             :episode_id [> 10]
                             :episode_id [<= 15]))
              (map :episode_id)
              set)))

  (is (= (set (range 0 15))
         (->> (select :tv_series
                      (where :series_title "Futurama"
                             :episode_id [< 15]))
              (map :episode_id)
              set)))

  (drop-table :tv_series))


(deftest test-paginate
  (create-table :tv_series
                (column-definitions {:series_title  :varchar
                                     :episode_id    :int
                                     :episode_title :text
                                     :primary-key [:series_title :episode_id]}))
  (dotimes [i 20]
    (insert :tv_series {:series_title "Futurama" :episode_id i :episode_title (str "Futurama Title " i)})
    (insert :tv_series {:series_title "Simpsons" :episode_id i :episode_title (str "Simpsons Title " i)}))

  (is (= (set (range 0 10))
         (->> (select :tv_series
                      (paginate :key :episode_id :per-page 10 :where { :series_title "Futurama"}))
              (map :episode_id)
              set)))

  (is (= (set (range 11 20))
         (->> (select :tv_series
                      (paginate :key :episode_id :per-page 10 :last-key 10 :where { :series_title "Futurama"}))
              (map :episode_id)
              set)))

  (drop-table :tv_series))

(deftest test-insert-with-consistency-level
  (testing "New DSL"
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (client/with-consistency-level :quorum
         (insert :users r))
       (is (= r (get-one :users)))
       (truncate :users))))
  (testing "Old DSL"
    (th/test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (client/with-consistency-level (client/consistency-level :quorum)
         (insert :users r))
       (is (= r (get-one :users)))
       (truncate :users)))))

;; think about using `cons/conj` as a syntax sugar for prepended and appended list commands
;; test authentication

(deftest test-raw-cql-insert
  (testing "With default session"
    (client/execute "INSERT INTO users (name, city, age) VALUES ('Alex', 'Munich', 19);")
    (is (= {:name "Alex" :city "Munich" :age (int 19)}
           (first (client/execute "SELECT * FROM users;"))))
    (client/execute "TRUNCATE users;"))
  (testing "Prepared statement"
    (client/execute (client/as-prepared "INSERT INTO users (name, city, age) VALUES (?, ?, ?);"
                                        "Alex" "Munich" (int 19))
                    :prepared true)
    (is (= {:name "Alex" :city "Munich" :age (int 19)}
           (first (client/execute "SELECT * FROM users;"))))
    (client/execute "TRUNCATE users;")))

(deftest test-insert-nils
  (client/prepared
   (let [r {:name "Alex" :city "Munich" :age nil}]
     (insert :users r)
     (is (= r (first (select :users)))))))
