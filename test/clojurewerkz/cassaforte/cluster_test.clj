(ns clojurewerkz.cassaforte.cluster-test
  (:use clojurewerkz.cassaforte.cluster
        clojure.test
        clojurewerkz.cassaforte.cql.query
        clojurewerkz.cassaforte.test-helper
        clojurewerkz.cassaforte.conversion
        )
  (:require [clojurewerkz.cassaforte.bytes :as bb])
  (:import [com.datastax.driver.core DataType DataType$Name]
           [clojurewerkz.cassaforte Codec]))

(defn init-keyspace
  [f]
  (create-keyspace :new_cql_keyspace
                   (with {:replication
                          {:class "SimpleStrategy"
                           :replication_factor 1 }}))

  (use-keyspace :new_cql_keyspace)
  (f)
  (drop-keyspace :new_cql_keyspace))

(use-fixtures :each initialize-cluster init-keyspace)

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

  (println
   (into []
         (for [row (select :posts
                           (where :userid "user1"
                                  :posted_at [> "2011-01-05"]))]
           (into {}
                 (for [cd (.getColumnDefinitions row)]
                   (let [n (.getName cd)]
                     [n (bb/deserialize2 (Codec/getCodec (.getType cd)) (.getBytesUnsafe row n))]
                     )))))))
