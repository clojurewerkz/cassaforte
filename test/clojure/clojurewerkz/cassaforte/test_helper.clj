(ns clojurewerkz.cassaforte.test-helper
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.cql :refer :all]
            [clojurewerkz.cassaforte.query.dsl :refer :all]))

(declare ___test-session)

(defn make-test-session
  []
  (defonce ___test-session (client/connect ["127.0.0.1"]))
  ___test-session)

(defn with-temporary-keyspace
  [f]

  (let [session (make-test-session)]

    (drop-keyspace session :new_cql_keyspace (if-exists))

    (create-keyspace session "new_cql_keyspace"
                     (with {:replication
                            {:class "SimpleStrategy"
                             :replication_factor 1 }}))

    (use-keyspace session :new_cql_keyspace)

    (create-table session :users
                  (column-definitions {:name :varchar
                                       :age  :int
                                       :city :varchar
                                       :primary-key [:name]}))

    ;; Same table as users, used for copying and such
    (create-table session :users2
                  (column-definitions {:name :varchar
                                       :age  :int
                                       :city :varchar
                                       :primary-key [:name]}))

    (create-table session :user_posts
                  (column-definitions {:username :varchar
                                       :post_id  :varchar
                                       :body     :text
                                       :primary-key [:username :post_id]}))

    (create-table session :user_counters
                  (column-definitions {:name :varchar
                                       :user_count  :counter
                                       :primary-key [:name]}))

    (create-table session :events_by_device_id_and_date
                  (column-definitions {:date        :varchar
                                       :device_id   :varchar
                                       :created_at  :timeuuid
                                       :payload     :text
                                       :primary-key [[:device_id :date] :created_at]}))

    (f)
    (drop-keyspace session :new_cql_keyspace)))
