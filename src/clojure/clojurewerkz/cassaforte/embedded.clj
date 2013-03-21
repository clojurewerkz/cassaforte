(ns clojurewerkz.cassaforte.embedded
  (:import [org.apache.cassandra.service CassandraDaemon]
           [org.apache.cassandra.config DatabaseDescriptor]))

(declare daemon)

(defn start-server!
  [config-path]
  (System/setProperty "cassandra.config" config-path)
  (System/setProperty "cassandra-foreground" "yes")
  (System/setProperty "log4j.defaultInitOverride" "false")

  (.start (Thread. ^Runnable (fn []
                               (let [d (CassandraDaemon.)]
                                 (defonce daemon d)
                                 (.init d nil)
                                 (.start d))
                                 ))))
(comment
  (defn start-server!
    []
    (.start (Thread. ^Runnable (fn []
                                 (println "starting")
                                 (defonce daemon
                                   (doto (EmbeddedCassandraService.)
                                     (.start))
                                   ))))))

;; (start-server! "/Users/ifesdjeen/p/clojurewerkz/cassaforte/resources/cassandra.yaml")
;; (DatabaseDescriptor/getRpcAddress)