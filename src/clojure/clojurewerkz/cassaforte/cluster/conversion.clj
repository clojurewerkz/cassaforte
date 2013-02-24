(ns clojurewerkz.cassaforte.cluster.conversion
  (:import [com.datastax.driver.core ResultSet]
           [clojurewerkz.cassaforte Codec])
  (:require [clojurewerkz.cassaforte.conversion :as conv]
            [clojurewerkz.cassaforte.bytes :as b]))

(extend-protocol conv/DefinitionToMap
  ResultSet
  (to-map [^ResultSet input]
    (into []
          (for [row input]
            (into {}
                  (for [cd (.getColumnDefinitions row)]
                    (let [n (.getName cd)
                          bytes (.getBytesUnsafe row n)]
                      [(keyword n) (when bytes
                              (b/deserialize-intern (Codec/getCodec (.getType cd))
                                                    (b/to-bytes (.getBytesUnsafe row n))))])))))))
