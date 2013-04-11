(ns clojurewerkz.cassaforte.cql-test-test
  (:require [clojurewerkz.cassaforte.embedded :as e])
  (:use clojurewerkz.cassaforte.cql
        clojure.test
        clojurewerkz.cassaforte.query
        clojurewerkz.cassaforte.conversion))

(declare cluster-client)

(defn run!
  [f]
  (create-keyspace :new_cql_keyspace
                   (with {:replication
                          {:class "SimpleStrategy"
                           :replication_factor 1 }}))
  (use-keyspace :new_cql_keyspace)
  (f)
  (drop-keyspace :new_cql_keyspace))

(defn initialize!
  [f]
  (e/start-server! "/Users/ifesdjeen/p/clojurewerkz/cassaforte/resources/cassandra.yaml")
  (defonce cluster-client (connect! ["127.0.0.1"]))
  ;; (defonce cluster-client (connect! ["192.168.60.10" "192.168.60.11" "192.168.60.12"]))
  (println 123)
  (with-client cluster-client
    (run! f)))

(use-fixtures :each initialize!)

(deftest test-range-queries
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
            (using :timestamp (.getTime (new java.util.Date))
                   :ttl 200000))
    (comment
      (prepared
       (insert :posts
               (values {:userid "user2"
                        :posted_at (java.util.Date. 112 0 i 1 0 0)
                        :entry_title (str "title" i)
                        :content (str "content" i)})
               (using :timestamp (.getTime (new java.util.Date))
                      :ttl 200000)))))

  (println (select :posts
                   (where :userid "user1"
                          :posted_at [> "2011-01-05"])))
  (comment
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
      (is (= 18 (perform-count :posts
                               (where :userid [:in ["user1" "user2"]]
                                      :posted_at [> "2011-01-01"]))))
      (is (= 16 (perform-count :posts
                               (where :userid [:in ["user1" "user2"]]
                                      :posted_at [> "2012-01-01"]))))

      (is (= 6 (perform-count :posts
                              (where :userid [:in ["user1" "user2"]]
                                     :posted_at [> "2012-01-01"]
                                     :posted_at [< "2012-01-05"]))))
      (is (= 10 (perform-count :posts
                               (where :userid [:in ["user1" "user2"]]
                                      :posted_at [>= "2012-01-01"]
                                      :posted_at [<= "2012-01-05"]))))

      (is (= 18 (perform-count :posts
                               (where :userid [:in ["user1" "user2"]]
                                      :posted_at [> "2011-01-01"])))))
    ))