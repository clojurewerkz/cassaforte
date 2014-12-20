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
