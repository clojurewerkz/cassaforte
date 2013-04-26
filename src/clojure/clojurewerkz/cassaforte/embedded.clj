(ns clojurewerkz.cassaforte.embedded
  (:require [clojure.java.io :as io])
  (:import [org.apache.cassandra.service CassandraDaemon]
           [org.apache.cassandra.config DatabaseDescriptor]))

(declare daemon)

(defn start-server!
  []
  (System/setProperty "cassandra.config" (str (io/resource "cassandra.yaml")))
  (System/setProperty "java.version" "1.7.0_15") ;; WTF
  (System/setProperty "cassandra-foreground" "yes")
  (System/setProperty "log4j.defaultInitOverride" "false")
  (System/setProperty "log4j.appender.R.File" "/var/log/cassandra/system.log")

  (when-not (bound? (var daemon))
    (.delete (java.io.File. "tmp"))
    (def daemon (let [d (CassandraDaemon.)]
                  (.init d nil)
                  (.start d)
                  d))))


(defn stop-server!
  []
  (.stop ^CassandraDaemon daemon))
