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

(ns clojurewerkz.cassaforte.uuids
  "Provides utility functions for UUID generation."
  (:import com.datastax.driver.core.utils.UUIDs
           java.util.UUID))

(defn ^UUID random
  "Creates a new random (version 4) UUID."
  []
  (UUID/randomUUID))

(defn ^UUID time-based
  "Creates a new time-based (version 1) UUID."
  []
  (UUIDs/timeBased))

(defn ^UUID start-of
  "Creates a \"fake\" time-based UUID that sorts as the smallest possible
version 1 UUID generated at the provided timestamp.

   Timestamp must be a unix timestamp."
  [^long timestamp]
  (UUIDs/startOf timestamp))

(defn ^UUID end-of
  "Creates a \"fake\" time-based UUID that sorts as the biggest possible
version 1 UUID generated at the provided timestamp.

   Timestamp must be a unix timestamp."
  [^long timestamp]
  (UUIDs/endOf timestamp))

(defn ^long unix-timestamp
  "Return the unix timestamp contained by the provided time-based UUID."
  [^UUID uuid]
  (UUIDs/unixTimestamp uuid))
