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

(ns clojurewerkz.cassaforte.utils
  (:refer-clojure :exclude [update])
  (:require [clojurewerkz.cassaforte.cql :refer :all]))

(defn- group-map
  "Groups by and applies f to each group"
  [k v f]
  (->> v
       (group-by k)
       (map (fn [[kk vv]]
              [kk (f (first vv))]))
       (apply concat)
       (apply hash-map)))

(defn transform-dynamic-table
  "Brings dynamic table to its intuitive representation

  Converts plain table structure:

    | :metric |                         :time | :value_1 | :value_2 |
    |---------+-------------------------------+----------+----------|
    | metric1 | Sun May 12 23:46:25 CEST 2013 |  val_1_1 |  val_1_2 |
    | metric1 | Mon May 13 00:07:51 CEST 2013 |  val_2_1 |  val_2_2 |

   To something that looks more like a tree:

    {\"metric1\"
     {#inst \"2013-05-12T21:46:25.947-00:00\"
      {:value_1 \"val_1_1\", :value_2 \"val_1_2\"},
      #inst \"2013-05-12T22:07:51.276-00:00\"
      {:value_1 \"val_2_1\", :value_2 \"val_2_2\"}}}

    |   key   |                                          row                                           |
    |---------+-------------------------------------------+--------------------------------------------+
    |         |       Sun May 12 23:46:25 CEST 2013       |       Mon May 13 00:07:51 CEST 2013        |
    | metric1 +-------------------------------------------+--------------------------------------------+
    |         |  value_1=val_1_1   |  value_2=val_1_2     |   value_1=val_2_1   |  value_2=val_2_2     |
    |---------+-------------------------------------------+--------------------------------------------+
"
  [coll partition-key key-part]
  (into {}
        (for [[k v] (group-by partition-key coll)]
          [k
           (group-map key-part v
                      #(dissoc % partition-key key-part))])))
