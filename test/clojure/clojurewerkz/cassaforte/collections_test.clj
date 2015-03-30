(ns clojurewerkz.cassaforte.collections-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.test-helper :as th]
            [clojurewerkz.cassaforte.client      :as client]
            [clojurewerkz.cassaforte.cql         :as cql :refer :all]

            [clojure.test                        :refer :all]

            [clojurewerkz.cassaforte.query       :refer :all]))

(let [s (th/make-test-session)]
  (use-fixtures :each (fn [f]
                        (th/with-temporary-keyspace s f)))
  (deftest test-collection-conversion-on-load
    (let [t :users_collections]
      (create-table s t
                    (column-definitions
                     {:name :varchar
                      :test_map (map-type :varchar :varchar)
                      :test_set (set-type :int)
                      :test_list (list-type :varchar)
                      :primary-key [:name]}))

      (insert s t
              {:name "user1"
               :test_map {"a" "b" "c" "d"}
               :test_set #{1 2 3}
               :test_list ["clojure" "cassandra"]})
      (let [m  (-> (select s t) first)
            mv (:test_map m)
            ms (:test_set m)
            ml (:test_list m)]
        (is (map? mv))
        (is (= {"a" "b" "c" "d"} mv))
        (is (set? ms))
        (is (= #{1 2 3} ms))
        (is (vector? ml))
        (is (= ["clojure" "cassandra"] ml)))
      (truncate s t)
      (drop-table s t)))


  (deftest test-list-operations
    (create-table s :users_list
                  (column-definitions
                   {:name :varchar
                    :test_list (list-type :varchar)
                    :primary-key [:name]}))

    (testing "Inserting"
      (insert s :users_list
              {:name "user1"
               :test_list ["str1" "str2" "str3"]})
      (is (= ["str1" "str2" "str3"] (get-in (select s :users_list)
                                            [0 :test_list])))
      (truncate s :users_list))

    (testing "Updating"
      (insert s :users_list
              {:name "user1"
               :test_list []})
      (dotimes [i 3]
        (update s :users_list
                {:test_list [+ [(str "str" i)]]}
                (where {:name "user1"})))

      (is (= ["str0" "str1" "str2"] (get-in (select s :users_list)
                                            [0 :test_list])))
      (truncate s :users_list))

    (testing "Deleting"
      (insert s :users_list
              {:name "user1"
               :test_list ["str0" "str1" "str2"]})
      (update s :users_list
              {:test_list [- ["str0" "str1"]]}
              (where {:name "user1"}))

      (is (= ["str2"] (get-in (select s :users_list)
                              [0 :test_list]))))

    (drop-table s :users_list))

  (deftest test-map-operations
    (create-table s :users_map
                  (column-definitions
                   {:name :varchar
                    :test_map (map-type :varchar :varchar)
                    :primary-key [:name]}))

    (testing "Inserting"
      (insert s :users_map
              {:name "user1"
               :test_map {"a" "b" "c" "d"}})
      (is (= {"a" "b" "c" "d"} (get-in (select s :users_map)
                                       [0 :test_map])))
      (truncate s :users_map))

    (testing "Updating"
      (insert s :users_map
              {:name "user1"
               :test_map {}})
      (dotimes [i 3]
        (update s :users_map
                {:test_map [+ {"a" "b" "c" "d"}]}
                (where {:name "user1"})))

      (is (= {"a" "b" "c" "d"} (get-in (select s :users_map)
                                       [0 :test_map])))
      (truncate s :users_map))

    (testing "Deleting"
      (insert s :users_map
              {:name "user1"
               :test_map {"a" "b" "c" "d"}})
      (delete s :users_map
              (columns {:test_map "c"})
              (where [[= :name "user1"]]))
      (is (= {"a" "b"} (get-in (select s :users_map)
                               [0 :test_map])))
      (truncate s :users_map))
    (drop-table s :users_map))


  (deftest test-set-operations
    (create-table s :users_set
                  (column-definitions
                   {:name :varchar
                    :test_set (set-type :varchar)
                    :primary-key [:name]}))

    (testing "Inserting"
      (insert s :users_set
              {:name "user1"
               :test_set #{"str1" "str2" "str3"}})
      (is (= #{"str1" "str2" "str3"} (get-in (select s :users_set)
                                             [0 :test_set])))
      (truncate s :users_set))


    (testing "Updating"
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
      (truncate s :users_set))

    (testing "Deleting"
      (insert s :users_set
              {:name "user1"
               :test_set #{"str0" "str1" "str2"}})
      (update s :users_set
              {:test_set [- #{"str0" "str1"}]}
              (where {:name "user1"}))

      (is (= #{"str2"} (get-in (select s :users_set)
                               [0 :test_set]))))

    (drop-table s :users_set)))
