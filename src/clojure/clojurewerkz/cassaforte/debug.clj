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

(ns clojurewerkz.cassaforte.debug
  "Debug utilities"
  (:require [clojure.stacktrace :refer :all]))

(defmacro catch-exceptions
  "Catches driver exceptions and outputs stacktrace."
  [& forms]
  `(try
     (do ~@forms)
     (catch com.datastax.driver.core.exceptions.DriverException ire#
       (println (.getMessage ire#))
       (print-cause-trace ire#))))
