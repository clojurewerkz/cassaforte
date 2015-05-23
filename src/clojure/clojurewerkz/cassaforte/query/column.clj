(ns clojurewerkz.cassaforte.query.column)

(defn write-time
  "Selects the write time of provided column"
  [column]
  (fn writetime-query [query-builder]
    (.writeTime query-builder (name column))))

(defn ttl-column
  "Selects the ttl of provided column."
  [column]
  (fn ttl-query [query-builder]
    (.ttl query-builder (name column))))

(defn distinct*
  ""
  [column]
  (fn distinct-query [query-builder]
    (.distinct (.column query-builder column))))

(defn as
  [wrapper alias]
  (fn distinct-query [query-builder]
    (.as (wrapper query-builder) alias)))
