;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns clojurewerkz.cassaforte.conversion
  (:import [com.datastax.driver.core ResultSet Host Row ColumnDefinitions ColumnDefinitions
            ColumnDefinitions$Definition]
           [java.nio ByteBuffer])
  (:require [clojurewerkz.cassaforte.bytes :as b]))

;; Protocol version 2, requires Cassandra 2.0+.
(def ^:const protocol-version 2)

(defprotocol ClojureRepresentation
  (to-clj [input] "Converts any definition to a Clojure data structure"))

(extend-protocol ClojureRepresentation
  ResultSet
  (to-clj [^ResultSet input]
    (into []
          (for [^Row row input]
            (into {}
                  (for [^ColumnDefinitions$Definition cd (.getColumnDefinitions row)]
                    (let [^String n         (.getName cd)
                          ^ByteBuffer bytes (.getBytesUnsafe row n)]
                      [(keyword n) (when (and bytes (> (.capacity bytes) 0))
                                     (b/deserialize (.getType cd)
                                                    bytes
                                                    protocol-version))]))))))
  Host
  (to-clj [^Host host]
    {:datacenter (.getDatacenter host)
     :address    (.getHostAddress (.getAddress host))
     :rack       (.getRack host)}))
