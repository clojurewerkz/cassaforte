;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

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
