(ns clojurewerkz.cassaforte.conversion
  (:import [com.datastax.driver.core ResultSet Host Row ColumnDefinitions ColumnDefinitions
            ColumnDefinitions$Definition]
           [java.nio ByteBuffer])
  (:require [clojurewerkz.cassaforte.bytes :as b]))

(defprotocol DefinitionToMap
  (to-map [input] "Converts any definition to map"))

(extend-protocol DefinitionToMap
  ResultSet
  (to-map [^ResultSet input]
    (into []
          (for [^Row row input]
            (into {}
                  (for [^ColumnDefinitions$Definition cd (.getColumnDefinitions row)]
                    (let [^String n         (.getName cd)
                          ^ByteBuffer bytes (.getBytesUnsafe row n)]
                      [(keyword n) (when bytes
                                     (b/deserialize (.getType cd)
                                                    (.getBytesUnsafe row n)))]))))))
  Host
  (to-map [^Host host]
    {:datacenter (.getDatacenter host)
     :address    (.getHostAddress (.getAddress host))
     :rack       (.getRack host)}))
