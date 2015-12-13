(ns clojurewerkz.cassaforte.conversion-test
  (:import [java.util HashMap ArrayList HashSet])
  (:require [clojurewerkz.cassaforte.conversion :refer :all]
            [clojurewerkz.cassaforte.client :as client]

            [clojurewerkz.cassaforte.test-helper :refer [^Session *session* with-keyspace]]
            [clojure.test :refer :all]))

(use-fixtures :each with-keyspace)

(deftest ints-and-strings
  (client/execute *session* "create table test1(id int primary key, val varchar)")
  (client/execute *session* "insert into test1(id, val) values (1, 'whatsup')")
  (is (= [{:id 1 :val "whatsup"}]
         (to-clj (client/execute *session* "select * from test1")))))
(deftest list-of-ints
  (client/execute *session* "create table test1(id int primary key, val list<int>)")
  (client/execute *session* "insert into test1(id, val) values (1, [5, 6, 7])")
  (is (= [{:id 1 :val [5 6 7]}]
         (to-clj (client/execute *session* "select * from test1")))))
(deftest set-of-doubles
  (client/execute *session* "create table test1(id int primary key, val set<double>)")
  (client/execute *session* "insert into test1(id, val) values (1, {5.0, 6.5, 7.314})")
  (is (= [{:id 1 :val #{5.0 6.5 7.314}}]
         (to-clj (client/execute *session* "select * from test1")))))
(deftest map-of-stuff
  (client/execute *session* "create table test1(id int primary key, val map<timestamp, inet>)")
  (client/execute *session* "insert into test1(id, val) values (1, {
                      '2015-10-10 00:00:00+0000' : '127.0.0.1',
                      '2015-11-11 11:11:11+0000' : '8.8.8.8'})")
  (is (= [{:id 1 :val {#inst "2015-10-10T00:00:00.000-00:00" (java.net.InetAddress/getByName "127.0.0.1")
                       #inst "2015-11-11T11:11:11.000-00:00" (java.net.InetAddress/getByName "8.8.8.8")}}]
         (to-clj (client/execute *session* "select * from test1")))))
(deftest nils-everywhere
  (client/execute *session* "create table test1(id int primary key, val1 int, val2 text, val3 list<inet>)")
  (client/execute *session* "insert into test1(id, val1, val2, val3) values (1, null, null, null)")
  (is (= [{:id 1 :val1 nil :val2 nil :val3 []}]
         (to-clj (client/execute *session* "select * from test1")))))

(deftest conversion-test
  (testing "Map Conversion"
    (let [java-map (HashMap. )]
      (doto java-map
        (.put "a" 1)
        (.put "b" 2)
        (.put "c" 3))
      (is (= {"a" 1
              "b" 2
              "c" 3}
             (to-clj java-map)))))

  (testing "Empty Map Conversion"
    (let [java-map (HashMap. )]
      (is (= {}
             (to-clj java-map)))))

  (testing "List Conversion"
    (let [java-list (ArrayList. )]
      (doto java-list
        (.add "a")
        (.add "b")
        (.add "c"))
      (is (= ["a" "b" "c"]
             (to-clj java-list)))))

  (testing "Empty List Conversion"
    (let [java-list (ArrayList. )]
      (is (= []
             (to-clj java-list)))))

    (testing "List Conversion"
    (let [java-list (HashSet. )]
      (doto java-list
        (.add "a")
        (.add "b")
        (.add "c"))
      (is (= #{"a" "b" "c"}
             (to-clj java-list)))))

  (testing "Empty List Conversion"
    (let [java-list (HashSet. )]
      (is (= #{}
             (to-clj java-list))))))
