(ns clojurewerkz.cassaforte.utils)

(defmacro with-thrift-exception-handling
  [& forms]
  `(try
     (do ~@forms)
     (catch org.apache.cassandra.thrift.InvalidRequestException ire#
       (println (.getWhy ire#)))))

;; add switch for:
;; (defn *throw-exceptions* false)
(defmacro with-native-exception-handling
  [& forms]
  `(try
     (do ~@forms)
     (catch com.datastax.driver.core.exceptions.DriverException ire#
       (println (.getMessage ire#)))
     (catch org.apache.cassandra.exceptions.CassandraException ire#
       (println (.getMessage ire#)))))
