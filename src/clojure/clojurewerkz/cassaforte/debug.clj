(ns clojurewerkz.cassaforte.debug
  "Debug utilities"
  (:use clojure.stacktrace))

(defmacro output-debug
  "Prints debugging statements out."
  [q]
  `(do
     (println "Built query: " ~q)
     ~q))

(defmacro catch-exceptions
  "Catches driver exceptions and outputs stacktrace."
  [& forms]
  `(try
     (do ~@forms)
     (catch com.datastax.driver.core.exceptions.DriverException ire#
       (println (.getMessage ire#))
       (print-cause-trace ire#))))
