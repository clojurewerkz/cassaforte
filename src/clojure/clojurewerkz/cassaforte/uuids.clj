(ns clojurewerkz.cassaforte.uuids
  "Provides utility methods to work with UUID."
  (:import [com.datastax.driver.core.utils UUIDs]
           [java.util UUID]))

(defn random
  "Creates a new random (version 4) UUID."
  []
  (UUID/randomUUID))

(defn time-based
  "Creates a new time-based (version 1) UUID."
  []
  (UUIDs/timeBased))

(defn start-of
  "Creates a \"fake\" time-based UUID that sorts as the smallest possible
version 1 UUID generated at the provided timestamp.

   Timestamp must be a unix timestamp."
  [^long timestamp]
  (UUIDs/startOf timestamp))

(defn end-of
  "Creates a \"fake\" time-based UUID that sorts as the biggest possible
version 1 UUID generated at the provided timestamp.

   Timestamp must be a unix timestamp."
  [^long timestamp]
  (UUIDs/endOf timestamp))

(defn unix-timestamp
  "Return the unix timestamp contained by the provided time-based UUID."
  [^UUID uuid]
  (UUIDs/unixTimestamp uuid))
