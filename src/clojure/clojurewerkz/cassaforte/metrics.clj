;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

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
