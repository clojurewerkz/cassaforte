(ns clojurewerkz.cassaforte.utils)

(defmacro with-thrift-exception-handling
  [& forms]
  `(try
     (do ~@forms)
     (catch org.apache.cassandra.thrift.InvalidRequestException ire#
       (println (.getWhy ire#)))))

(defmacro with-native-exception-handling
  [& forms]
  `(try
     (do ~@forms)
     (catch Exception ire#
       (println (.getMessage ire#)))))
