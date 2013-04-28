(ns clojurewerkz.cassaforte.embedded
  (:require [clojure.java.io :as io])
  (:import [org.apache.cassandra.service CassandraDaemon]
           [org.apache.cassandra.config DatabaseDescriptor]))

(defn delete-file
  "Delete file f. Raise an exception if it fails unless silently is true."
  [f & [silently]]
  (or (.delete (io/file f))
      silently
      (throw (java.io.IOException. (str "Couldn't delete " f)))))

(defn delete-file-recursively
  "Delete file f. If it's a directory, recursively delete all its contents.
Raise an exception if any deletion fails unless silently is true."
  [f & [silently]]
  (let [f (io/file f)]
    (if (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-file-recursively child silently)))
    (delete-file f silently)))

(declare daemon)

(defn start-server!
  []
  (System/setProperty "cassandra.config" (str (io/resource "cassandra.yaml")))
  (System/setProperty "java.version" "1.7.0_15") ;; WTF
  (System/setProperty "cassandra-foreground" "yes")
  (System/setProperty "log4j.defaultInitOverride" "false")
  (System/setProperty "log4j.appender.R.File" "/var/log/cassandra/system.log")

  (when-not (bound? (var daemon))
    (delete-file-recursively "tmp")
    (def daemon (let [d (CassandraDaemon.)]
                  (.init d nil)
                  (.start d)
                  d))))


(defn stop-server!
  []
  (.stop ^CassandraDaemon daemon))
