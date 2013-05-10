(ns clojurewerkz.cassaforte.cluster.conversion
  (:import [com.datastax.driver.core ResultSet Host Row ColumnDefinitions]
           [clojurewerkz.cassaforte Codec]
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
                  (for [^ColumnDefinitions cd (.getColumnDefinitions row)]
                    (let [^String n         (.getName cd)
                          ^ByteBuffer bytes (.getBytesUnsafe row n)]
                      [(keyword n) (when bytes
                                     (b/deserialize-intern (Codec/getCodec (.getType cd))
                                                           (b/to-bytes (.getBytesUnsafe row n))))]))))))
  Host
  (to-map [^Host host]
    {:datacenter (.getDatacenter host)
     :address    (.getHostAddress (.getAddress host))
     :rack       (.getRack host)})

  )
