(ns clojurewerkz.cassaforte.cql-test
  (:use clojurewerkz.cassaforte.cql
        clojure.test
        clojurewerkz.cassaforte.cql.query
        clojurewerkz.cassaforte.test-helper
        clojurewerkz.cassaforte.conversion))

(use-fixtures :each initialize-cql)

(deftest test-range-queries
  (drop-keyspace :new_cql_keyspace)
  (create-keyspace :new_cql_keyspace
                   (with {:replication
                          {:class "SimpleStrategy"
                           :replication_factor 1 }}))

  (use-keyspace :new_cql_keyspace)
  (create-table :posts
                (column-definitions {:userid :text
                                     :posted_at :timestamp
                                     :entry_title :text
                                     :content :text
                                     :primary-key [:userid :posted_at]}))

  (doseq [i (range 1 10)]
    (insert :posts
            (values {:userid "user1"
                     :posted_at (str "2012-01-0" i)
                     :entry_title (str "title" i)
                     :content (str "content" i)})
            (using :timestamp 100000
                   :ttl 200000))
    (prepared
     (insert :posts
            (values {:userid "user2"
                     :posted_at (java.util.Date. 112 0 i 1 0 0)
                     :entry_title (str "title" i)
                     :content (str "content" i)})
            (using :timestamp 100000
                   :ttl 200000))))

  (is (= "content1"
         (:content (first
                    (select :posts
                            (where :userid "user1"
                                   :posted_at [> "2011-01-05"]))))))

  (is (= "content9"
         (:content (first
                    (select :posts
                            (where :userid "user1"
                                   :posted_at [> "2011-01-05"])
                            (order-by [:posted_at :desc]))))))

  (testing "Range queries with open end"
    (is (= 9 (count (select :posts
                            (where :userid "user1"
                                   :posted_at [> "2011-01-01"])))))
    (is (= 8 (count (select :posts
                            (where :userid "user1"
                                   :posted_at [> "2012-01-01"]))))))

  (testing "Range queries"
    (is (= 3 (count (select :posts
                            (where :userid "user1"
                                   :posted_at [> "2012-01-01"]
                                   :posted_at [< "2012-01-05"])))))
    (is (= 5 (count (select :posts
                            (where :userid "user1"
                                   :posted_at [>= "2012-01-01"]
                                   :posted_at [<= "2012-01-05"]))))))


  (testing "Range queries and IN clause"
    (is (= 18 (count (select :posts
                             (where :userid [:in ["user1" "user2"]]
                                    :posted_at [> "2011-01-01"])))))
    (is (= 16 (count (select :posts
                             (where :userid [:in ["user1" "user2"]]
                                    :posted_at [> "2012-01-01"])))))
    (is (= 6 (count (select :posts
                            (where :userid [:in ["user1" "user2"]]
                                   :posted_at [> "2012-01-01"]
                                   :posted_at [< "2012-01-05"])))))
    (is (= 10 (count (select :posts
                             (where :userid [:in ["user1" "user2"]]
                                    :posted_at [>= "2012-01-01"]
                                    :posted_at [<= "2012-01-05"])))))

    (is (= 18 (count (select :posts
                             (where :userid [:in ["user1" "user2"]]
                                    :posted_at [> "2011-01-01"]))))))
  ;; (drop-keyspace :new_cql_keyspace)
  )

(deftest test-create-keyspace
  (drop-keyspace :new_cql_keyspace)
  (create-keyspace :new_cql_keyspace
                   (with {:replication
                          {:class "SimpleStrategy"
                           :replication_factor 1 }}))

  (let [ksdef (describe-keyspace :new_cql_keyspace)]
    (is (= "{\"replication_factor\":\"1\"}" (:strategy_options ksdef)))
    (is (= "org.apache.cassandra.locator.SimpleStrategy" (:strategy_class ksdef)))
    (is (= "new_cql_keyspace" (:keyspace_name ksdef)))))


(deftest test-create-table
  (drop-keyspace :new_cql_keyspace)
  (create-keyspace :new_cql_keyspace
                   (with {:replication
                          {:class "SimpleStrategy"
                           :replication_factor 1 }}))

  (create-table :posts
                (column-definitions {:userid :text
                                     :posted_at :timestamp
                                     :entry_title :text
                                     :content :text
                                     :primary-key [:userid :posted_at]}))

  (let [cfdef (describe-table :new_cql_keyspace :posts)]
    (= ["userid"] (:key_aliases cfdef))
    (= ["posted_at"] (:column_aliases cfdef))
    (= "posts" (:columnfamily_name cfdef))))
