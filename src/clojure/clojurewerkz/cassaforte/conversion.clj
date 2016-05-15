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
  (:import [com.datastax.driver.core ResultSet Host Row ColumnDefinitions ColumnDefinitions]
           [com.datastax.driver.core DataType DataType$Name CodecRegistry ProtocolVersion]
           [com.datastax.driver.core.exceptions DriverException]
           [java.nio ByteBuffer]
           [java.util Map List Set]))

(defn to-clj
  [java-val]
  (cond
    (instance? ResultSet java-val) (into [] ;; TODO: transient?
                                     (for [^Row row java-val]
                                       (let [^ColumnDefinitions cd (.getColumnDefinitions row)]
                                          (loop [row-data {} i (int 0)]
                                            (let [^String name (.getName cd i)
                                                  ^DataType data-type (.getType cd i)
                                                  value (to-clj (.get row i (.codecFor CodecRegistry/DEFAULT_INSTANCE data-type)))]
                                              (if (< (inc i) (.size cd))
                                                (recur (assoc row-data (keyword name) value) (inc i))
                                                (assoc row-data (keyword name) value)))))))
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
