(ns clojurewerkz.cassaforte.test-helper
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client    :as client]
            [clojurewerkz.cassaforte.cql       :refer :all]
            [clojurewerkz.cassaforte.query.dsl :refer :all]))

(def ^:dynamic *session*)

(def table-definitions
  {:tv_series {:series_title  :varchar
               :episode_id    :int
               :episode_title :text
               :primary-key [:series_title :episode_id]}})

(defn with-temporary-keyspace
  [f]
  (let [session (client/connect ["127.0.0.1"])]
    (try
      (drop-keyspace session :new_cql_keyspace (if-exists))

      (create-keyspace session "new_cql_keyspace"
                       (with {:replication
                              {"class"              "SimpleStrategy"
                               "replication_factor" 1 }}))

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

      (create-table session :events
                    (column-definitions {:message      :varchar
                                         :created_at   :timeuuid
                                         :primary-key  [:created_at]}))

      (create-table session :events_for_in_and_range_query
                    (column-definitions {:message      :varchar
                                         :city         :varchar
                                         :created_at   :timeuuid
                                         :primary-key  [:message :created_at]}))

      (create-table session :events_by_device_id_and_date
                    (column-definitions {:date        :varchar
                                         :device_id   :varchar
                                         :created_at  :timeuuid
                                         :payload     :text
                                         :primary-key [[:device_id :date] :created_at]}))

      (binding [*session* session]
        (f))
      (drop-keyspace session :new_cql_keyspace)
      (catch Exception e
        (throw e))
      (finally
        (client/disconnect! session)))
    ))

(defn with-keyspace
  [f]
  (let [session (client/connect ["127.0.0.1"])]
    (try
      (drop-keyspace session "new_cql_keyspace"
                     (if-exists))
      (create-keyspace session "new_cql_keyspace"
                       (with {:replication
                              {"class"              "SimpleStrategy"
                               "replication_factor" 1 }})
                       (if-not-exists))
      (use-keyspace session "new_cql_keyspace")

      (binding [*session* session]
        (f))

      (finally
        (drop-keyspace session "new_cql_keyspace"
                       (if-exists))
        (client/disconnect! session)))))

(defmacro with-table
  [table-name & body]
  `(do
     (create-table *session* ~table-name
                   (column-definitions (get table-definitions ~table-name)))
     ~@body
     (drop-table *session*  ~table-name)))
