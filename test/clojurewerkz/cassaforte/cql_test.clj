(ns clojurewerkz.cassaforte.cql-test
  (:require [clojurewerkz.cassaforte.embedded :as e])
  (:use clojurewerkz.cassaforte.cql
        clojure.test
        clojurewerkz.cassaforte.query
        clojurewerkz.cassaforte.test-helper
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
  (def cluster-client (connect! ["127.0.0.1"]))
  ;; (defonce cluster-client (connect! ["192.168.60.10" "192.168.60.11" "192.168.60.12"]))

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
    (prepared
     (insert :posts
             (values {:userid "user2"
                      :posted_at (java.util.Date. 112 0 i 1 0 0)
                      :entry_title (str "title" i)
                      :content (str "content" i)})
             (using :timestamp (.getTime (new java.util.Date))
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
                                    :posted_at [> "2011-01-01"]))))))

(deftest test-create-keyspace
  (let [ksdef (describe-keyspace :new_cql_keyspace)]
    (is (= "{\"replication_factor\":\"1\"}" (:strategy_options ksdef)))
    (is (= "org.apache.cassandra.locator.SimpleStrategy" (:strategy_class ksdef)))
    (is (= "new_cql_keyspace" (:keyspace_name ksdef)))))

(deftest test-alter-keyspace
  (alter-keyspace :new_cql_keyspace
                  (with {:durable_writes true}))

  (let [ksdef (describe-keyspace :new_cql_keyspace)]
    (is (:durable_writes ksdef))))

(deftest test-create-table
  (create-table :posts
                (column-definitions {:userid :text
                                     :posted_at :timestamp
                                     :entry_title :text
                                     :content :text
                                     :primary-key [:userid :posted_at]}))

  (let [cfdef (describe-table :new_cql_keyspace :posts)]
    (is (= "[\"userid\"]" (:key_aliases cfdef)))
    (is (= "[\"posted_at\"]" (:column_aliases cfdef)))
    (is (= "posts" (:columnfamily_name cfdef))))


  (let [cols (describe-columns :new_cql_keyspace :posts)]
    (is (= "content" (:column_name (first cols))))
    (is (= "entry_title" (:column_name (second cols))))))

(deftest test-alter-table
  (create-table :posts
                (column-definitions {:userid :text
                                     :to_be_int :text
                                     :primary-key [:userid]}))

  (let [cols (describe-columns :new_cql_keyspace :posts)]
    (is (= "org.apache.cassandra.db.marshal.UTF8Type" (:validator (first cols)))))

  (alter-table :posts
               (alter-column :to_be_int :int))

  (let [cols (describe-columns :new_cql_keyspace :posts)]
    (is (= "org.apache.cassandra.db.marshal.Int32Type" (:validator (first cols))))))


(deftest test-ttl
  (create-table :posts
                (column-definitions {:userid :text
                                     :content :text
                                     :primary-key [:userid]}))

  (insert :posts
          (values {:userid "user1"
                   :content "content"})
          (using :ttl 1))

  (Thread/sleep 2000)
  (is (= 0 (perform-count :posts))))

;; TODO: Cover count with query with tests
