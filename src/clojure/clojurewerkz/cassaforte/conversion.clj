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

(ns clojurewerkz.cassaforte.conversion
  (:import [com.datastax.driver.core ResultSet Host Row ColumnDefinitions ColumnDefinitions
            ColumnDefinitions$Definition]
           [com.datastax.driver.core DataType DataType$Name]
           [com.datastax.driver.core.exceptions DriverException]
           [java.nio ByteBuffer]
           [java.util Map List Set]))

;; Protocol version 2, requires Cassandra 2.0+.
(def ^:const protocol-version 2)

(defprotocol ClojureRepresentation
  (to-clj [input] "Converts any definition to a Clojure data structure"))


(defn deserialize
  [^DataType dt ^ByteBuffer bytes ^Integer protocol-version]
  (.deserialize dt bytes protocol-version))
;;
;; Core JDK Types
;;

(extend-protocol ClojureRepresentation
  nil
  (to-clj [input] nil)

  String
  (to-clj [^String input] input)

  Object
  (to-clj [input] input)

  Map
  (to-clj [^Map input]
    (into {} input))

  Set
  (to-clj [^Set input]
    (into #{} input))

  List
  (to-clj [^List input]
    (into [] input))

  DriverException
  (to-clj [^DriverException e]
    e))

;;
;; C* Types
;;

(extend-protocol ClojureRepresentation
  ResultSet
  (to-clj [^ResultSet input]
    (into []
          (for [^Row row input]
            (into {}
                  (for [^ColumnDefinitions$Definition cd (.getColumnDefinitions row)]
                    (let [^String n                      (.getName cd)
                          ^ByteBuffer bytes              (.getBytesUnsafe row n)]
                      [(keyword n) (when bytes
                                     (let [v (deserialize (.getType cd) bytes protocol-version)]
                                       (to-clj v)))]))))))
  Host
  (to-clj [^Host host]
    {:datacenter (.getDatacenter host)
     :address    (.getHostAddress (.getAddress host))
     :rack       (.getRack host)}))
