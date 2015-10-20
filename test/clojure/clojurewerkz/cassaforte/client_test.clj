(ns clojurewerkz.cassaforte.client-test
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.cql    :refer :all]
            [clojure.test                   :refer :all])
  (:import [com.datastax.driver.core.policies TokenAwarePolicy RoundRobinPolicy]))


(deftest ^:client test-disconnect
  (let [s (client/connect ["127.0.0.1"])
        cluster (.getCluster s)]
    (is (= (.isClosed s) false))
    (client/disconnect s)
    (is (= (.isClosed s) true))
    (is (= (.isClosed cluster) false))
    (client/shutdown-cluster cluster)
    (is (= (.isClosed cluster) true))))


(deftest ^:client test-disconnect!
  (let [s (client/connect ["127.0.0.1"])
        cluster (.getCluster s)]
    (is (= (.isClosed s) false))
    (is (= (.isClosed cluster) false))
    (client/disconnect! s)
    (is (= (.isClosed s) true))
    (is (= (.isClosed cluster) true))))


(deftest ^:client test-connect-via-uri
  (let [s (client/connect ["127.0.0.1"])]
        (try
          (drop-keyspace s :new_cql_keyspace)
          (catch Exception _ nil))
        (create-keyspace s "new_cql_keyspace"
                         (with {:replication
                                {"class"              "SimpleStrategy"
                                 "replication_factor" 1 }}))
        (let [s2 (client/connect-with-uri "cql://127.0.0.1:9042/new_cql_keyspace")]
          (is (= (.isClosed s2) false))
          (client/disconnect s2)
          (is (= (.isClosed s2) true)))
        (client/disconnect s)))


(deftest ^:client test-connect
  (testing "Connect without options or keyspace"
    (let [session (client/connect ["127.0.0.1"])
          cluster (.getCluster session)]
      (is (= false (.isClosed session)))
      (is (= false (.isClosed cluster)))
      (is (= nil (.getLoggedKeyspace session)))
      (is (= TokenAwarePolicy (class (.. cluster getConfiguration getPolicies getLoadBalancingPolicy))))
      (client/disconnect session)))

  (testing "Connect with keyspace, without options"
    (let [session (client/connect ["127.0.0.1"] "system")
          cluster (.getCluster session)]
      (is (= false (.isClosed session)))
      (is (= false (.isClosed cluster)))
      (is (= "system" (.getLoggedKeyspace session)))
      (is (= TokenAwarePolicy (class (.. cluster getConfiguration getPolicies getLoadBalancingPolicy))))
      (client/disconnect session)))

  (testing "Connect with keyspace in options"
    (let [session (client/connect ["127.0.0.1"] {:keyspace "system"})
          cluster (.getCluster session)]
      (is (= false (.isClosed session)))
      (is (= false (.isClosed cluster)))
      (is (= "system" (.getLoggedKeyspace session)))
      (is (= TokenAwarePolicy (class (.. cluster getConfiguration getPolicies getLoadBalancingPolicy))))
      (client/disconnect session)))

  (testing "Connect with options"
    (let [session (client/connect ["127.0.0.1"] {:load-balancing-policy (RoundRobinPolicy.)})
          cluster (.getCluster session)]
      (is (= false (.isClosed session)))
      (is (= false (.isClosed cluster)))
      (is (= nil (.getLoggedKeyspace session)))
      (is (= RoundRobinPolicy (class (.. cluster getConfiguration getPolicies getLoadBalancingPolicy))))
      (client/disconnect session)))

  (testing "Connect with options and keyspace (in options)"
    (let [session (client/connect ["127.0.0.1"] {:load-balancing-policy (RoundRobinPolicy.) :keyspace "system"})
          cluster (.getCluster session)]
      (is (= false (.isClosed session)))
      (is (= false (.isClosed cluster)))
      (is (= "system" (.getLoggedKeyspace session)))
      (is (= RoundRobinPolicy (class (.. cluster getConfiguration getPolicies getLoadBalancingPolicy))))
      (client/disconnect session)))

  (testing "Connect with options and keyspace (separate args)"
    (let [session (client/connect ["127.0.0.1"] "system" {:load-balancing-policy (RoundRobinPolicy.)})
          cluster (.getCluster session)]
      (is (= false (.isClosed session)))
      (is (= false (.isClosed cluster)))
      (is (= "system" (.getLoggedKeyspace session)))
      (is (= RoundRobinPolicy (class (.. cluster getConfiguration getPolicies getLoadBalancingPolicy))))
      (client/disconnect session))))
