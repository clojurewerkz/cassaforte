(ns clojurewerkz.cassaforte.thrift.query-builders
  (:use    [clojurewerkz.cassaforte.thrift.column-or-super-column :only [build-cosc]]
           [clojurewerkz.cassaforte.bytes :only [encode]])
  (:import [org.apache.cassandra.thrift Mutation SliceRange ColumnParent SlicePredicate ColumnPath]))

(defn build-mutation
  [cosc]
  (.setColumn_or_supercolumn (Mutation.) (build-cosc cosc)))

(defn build-slice-range
  "A SliceRange is a structure that stores basic range, ordering and limit information for a query
   that will return multiple columns. It could be thought of as Cassandra's version of LIMIT and ORDER BY.

   Params:
     :start (binary) - The column name to start the slice with. This attribute is not required, though
                       there is no default value, and can be safely set to '', i.e., an empty byte array, to
                       start with the first column name. Otherwise, it must be a valid value under the rules
                       of the Comparator defined for the given ColumnFamily.

    :finish (binary) - The column name to stop the slice at. This attribute is not required,
                       though there is no default value, and can be safely set to an empty byte array to not
                       stop until count results are seen. Otherwise, it must also be a valid value to the
                       ColumnFamily Comparator.

    :reversed (bool) - Whether the results should be ordered in reversed order. Similar to
                      ORDER BY blah DESC in SQL. When reversed is true, start will determine the right end
                      of the range while finish will determine the left, meaning start must be >= finish.

    :count (integer), default is 100 - How many columns to return. Similar to LIMIT 100 in SQL.
                      May be arbitrarily large, but Thrift will materialize the whole result into memory before
                      returning it to the client, so be aware that you may be better served by iterating through
                      slices by passing the last value of one call in as the start of the next instead of increasing
                      count arbitrarily large."
  [^String start ^String finish & {:keys [count reversed]}]
  (let [slice-range ^SliceRange (doto (SliceRange.)
                      (.setStart (encode start))
                      (.setFinish (encode finish)))]
    (when count
      (.setCount slice-range count))
    (when reversed
      (.setReversed slice-range reversed))
    slice-range))

(defn build-slice-predicate
  [range]
  (doto (SlicePredicate.)
    (.setSlice_range range)))

(defn build-column-parent
  [^String column-family]
  (ColumnParent. column-family))

(defn build-column-path
  [^String column-family ^String field type]
  (let [column-path (ColumnPath. column-family)]
    (if (= type :super)
      (.setSuper_column column-path (encode field))
      (.setColumn column-path (encode field)))
    column-path))