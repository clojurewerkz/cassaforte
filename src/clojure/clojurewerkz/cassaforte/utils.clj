(ns clojurewerkz.cassaforte.utils
  (:use clojure.stacktrace))

;; add switch for:
;; (defn *throw-exceptions* false)

(defmacro with-native-exception-handling
  [& forms]
  `(try
     (do ~@forms)
     (catch com.datastax.driver.core.exceptions.DriverException ire#
       (println (print-stack-trace ire# 5))
       (println "Error came from server:" (.getMessage ire#)))
     (catch org.apache.cassandra.exceptions.CassandraException ire#
       (println "Error came from server:" (.getMessage ire#)))))
