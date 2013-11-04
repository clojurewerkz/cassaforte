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
