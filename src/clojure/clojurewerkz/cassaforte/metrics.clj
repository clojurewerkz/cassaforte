;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns clojurewerkz.cassaforte.metrics
  "Access to metrics (console, CSV, etc) for the driver"
  (:import [com.datastax.driver.core Session]
           [com.yammer.metrics.reporting ConsoleReporter CsvReporter]
           [com.yammer.metrics.core MetricsRegistry]
           [java.io File]
           [java.util.concurrent TimeUnit]))

(defn console-reporter
  [^Session client]
  (let [registry (-> client
                     .getCluster
                     .getMetrics
                     .getRegistry)]
    (ConsoleReporter/enable registry 1000 TimeUnit/SECONDS)))

(defn csv-reporter
  ([^Session client]
     (csv-reporter client "tmp/measurements" 1 TimeUnit/SECONDS))
  ([^Session client ^String dir ^long period ^TimeUnit time-unit]
     (let [registry (-> client
                        (.getCluster)
                        (.getMetrics)
                        (.getRegistry))
           f        (File. dir)
           _        (when (not (.exists f)) (.mkdir f))
           reporter (CsvReporter. registry f)]
       (.start reporter period time-unit)
       (.run reporter))))
