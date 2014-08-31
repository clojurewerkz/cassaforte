;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns clojurewerkz.cassaforte.bytes
  "Facility functions to use with serialization, handle deserialization of all the data types
   supported by Cassandra."
  (:import java.nio.ByteBuffer java.util.Date
           [com.datastax.driver.core DataType DataType$Name]))

(defn #^bytes to-bytes
  [^ByteBuffer byte-buffer]
  (let [bytes (byte-array (.remaining byte-buffer))]
    (.get byte-buffer bytes 0 (count bytes))
    bytes))

(defn deserialize
  [^DataType dt bytes ^Integer protocol-version]
  (.deserialize dt bytes protocol-version))
