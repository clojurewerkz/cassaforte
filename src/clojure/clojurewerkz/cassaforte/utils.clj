(ns clojurewerkz.cassaforte.utils
  (:refer [clojurewerkz.cassaforte.cql :refer :all]))

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
