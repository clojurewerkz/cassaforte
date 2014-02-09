;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns clojurewerkz.cassaforte.debug
  "Debug utilities"
  (:require [clojure.stacktrace :refer :all]))

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
