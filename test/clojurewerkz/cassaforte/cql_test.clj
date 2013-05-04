(ns clojurewerkz.cassaforte.cql-test
  (:require [clojurewerkz.cassaforte.embedded :as e]
            [clojurewerkz.cassaforte.test-helper :as th])
  (:use clojurewerkz.cassaforte.cql
        clojure.test
        clojurewerkz.cassaforte.query))

(use-fixtures :each th/initialize!)

(defmacro test-combinations [& body]
  `(do
     ~@body
     (prepared ~@body)))

(deftest insert-test
  (test-combinations
   (let [r {:name "Alex" :city "Munich" :age (int 19)}]
     (insert :users r)
     (is (= r (first (select :users))))
     (truncate :users))))

(deftest update-test
  (testing "Simple updates"
    (test-combinations
     (let [r {:name "Alex" :city "Munich" :age (int 19)}]
       (insert :users r)
       (is (= r (first (select :users))))
       (update :users
               {:age (int 25)}
               (where :name "Alex"))
       (is (= {:name "Alex" :city "Munich" :age (int 25)}
              (first (select :users)))))))

  (testing "One of many update"
    (test-combinations
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

(deftest delete-test
  (test-combinations
   (dotimes [i 3]
     (insert :users {:name (str "name" i) :age (int i)}))
   (is (= 3 (perform-count :users)))
   (delete :users
           (where :name "name1"))
   (is (= 2 (perform-count :users)))
   (truncate :users))

  (test-combinations
   (insert :users {:name "name1" :age (int 19)})
   (delete :users
           (columns :age)
           (where :name "name1"))
   (is (nil? (:age (select :users))))
   (truncate :users)))

(deftest ttl-test

  )

(deftest list-operations-test
  (create-table :users_list
                (column-definitions
                 {:name :varchar
                  :test_list (list-type :varchar)
                  :primary-key [:name]}))

  (testing "Inserting"
    (test-combinations
     (insert :users_list
             {:name "user1"
              :test_list ["str1" "str2" "str3"]})
     (is (= ["str1" "str2" "str3"] (get-in (select :users_list)
                                           [0 :test_list])))
     (truncate :users_list)))

  (testing "Updating"
    (test-combinations
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
    (test-combinations
     (insert :users_list
             {:name "user1"
              :test_list ["str0" "str1" "str2"]})
     (update :users_list
             {:test_list [- ["str0" "str1"]]}
             (where :name "user1"))

     (is (= ["str2"] (get-in (select :users_list)
                             [0 :test_list])))))

  (drop-table :users_list))

(deftest map-operations-test
  (create-table :users_map
                (column-definitions
                 {:name :varchar
                  :test_map (map-type :varchar :varchar)
                  :primary-key [:name]}))

  (testing "Inserting"
    (test-combinations
     (insert :users_map
             {:name "user1"
              :test_map {"a" "b" "c" "d"}})
     (is (= {"a" "b" "c" "d"} (get-in (select :users_map)
                                      [0 :test_map])))
     (truncate :users_map)))

  (testing "Updating"
    (test-combinations
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


(deftest set-operations-test
  (create-table :users_set
                (column-definitions
                 {:name :varchar
                  :test_set (set-type :varchar)
                  :primary-key [:name]}))

  (testing "Inserting"
    (test-combinations
     (insert :users_set
             {:name "user1"
              :test_set #{"str1" "str2" "str3"}})
     (is (= #{"str1" "str2" "str3"} (get-in (select :users_set)
                                            [0 :test_set])))
     (truncate :users_set)))


  (testing "Updating"
    (test-combinations
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
    (test-combinations
     (insert :users_set
             {:name "user1"
              :test_set #{"str0" "str1" "str2"}})
     (update :users_set
             {:test_set [- #{"str0" "str1"}]}
             (where :name "user1"))

     (is (= #{"str2"} (get-in (select :users_set)
                             [0 :test_set])))))

  (drop-table :users_set))

(deftest select-where-test
  (test-combinations
   (insert :users {:name "Alex"   :city "Munich"        :age (int 19)})
   (insert :users {:name "Robert" :city "Berlin"        :age (int 25)})
   (insert :users {:name "Sam"    :city "San Francisco" :age (int 21)})

   (is (= "Munich" (get-in (select :users (where :name "Alex")) [0 :city])))))

(deftest select-in-test
  (test-combinations
   (insert :users {:name "Alex"   :city "Munich"        :age (int 19)})
   (insert :users {:name "Robert" :city "Berlin"        :age (int 25)})
   (insert :users {:name "Sam"    :city "San Francisco" :age (int 21)})

   (let [users (select :users
                           (where :name [:in ["Alex" "Robert"]]))]
     (is (= "Munich" (get-in users [0 :city])))
     (is (= "Berlin" (get-in users [1 :city]))))))

(deftest select-order-by-test

  (test-combinations
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

(deftest select-range-query-test)

(deftest batch-test)
;; think about using `cons/conj` as a syntax sugar for prepended and appended list commands


;; test authentication
