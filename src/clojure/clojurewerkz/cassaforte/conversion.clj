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

(defn deserialize
  [^DataType dt ^ByteBuffer bytes ^Integer protocol-version]
  (.deserialize dt bytes protocol-version))

(defn to-clj
  [java-val]
  (cond
    (instance? ResultSet java-val) (into [] ;; TODO: transient?
                                         (for [^Row row java-val]
                                           (into {}
                                                 (for [^ColumnDefinitions$Definition cd (.getColumnDefinitions row)]
                                                   (let [^String n                      (.getName cd)
                                                         ^ByteBuffer bytes              (.getBytesUnsafe row n)]
                                                     [(keyword n) (when bytes
                                                                    (let [v (deserialize (.getType cd) bytes protocol-version)]
                                                                      ;; TODO Split to-clj to two parts for performance reasions:
                                                                      ;; The call dispatch "sources" aren't overlapping
                                                                      (to-clj v)))])))))
    (instance? Map java-val)       (let [t (transient {})]
                                     (doseq [[k v] java-val]
                                       (assoc! t k v))
                                     (persistent! t))
    (instance? Set java-val)       (let [t (transient #{})]
                                     (doseq [v java-val]
                                       (conj! t v))
                                     (persistent! t))
    (instance? List java-val)      (let [t (transient [])]
                                     (doseq [v java-val]
                                       (conj! t v))
                                     (persistent! t))
    (instance? Host java-val)      (let [^Host host java-val]
                                     {:datacenter (.getDatacenter host)
                                      :address    (.getHostAddress (.getAddress host))
                                      :rack       (.getRack host)})
    :else                          java-val))
