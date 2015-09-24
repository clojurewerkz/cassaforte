(ns clojurewerkz.cassaforte.query.dsl)

;;
;; SELECT Statement
;;

(defn count-all
  []
  [:what-count nil])

(defn fcall
  [name & args]
  [:what-fcall [name args]])

(defn all
  []
  ;; TODO: resolve API inconsistency
  [:what-all nil])

(defn columns
  [& columns]
  [:what-columns columns])

(defn column
  [column & keys]
  [:what-column [column keys]])

(defn where
  [m]
  [:where m])

(defn order-by
  [& orderings]
  [:order orderings])

(defn limit
  [lim]
  [:limit lim])

(defn allow-filtering
  []
  [:filtering nil])

(defn from
  [table-name]
  [:from (name table-name)])

;;
;; Insert Query
;;

(defn value
  [key value]
  [:value [key value]])

(defn if-not-exists
  []
  [:if-not-exists nil])

(defn using
  [m]
  [:using m])

;;
;; Update Statement
;;

(defn only-if
  [m]
  [:only-if m])

;;
;; Delete Statement
;;

(defn if-exists
  []
  [:if-exists nil])

;;
;; Alter Table
;;

(defn with
  [options]
  [:with-options options])

(defn add-column
  [column-name column-type]
  [:add-column [column-name column-type]])

(defn drop-column
  [column-name]
  [:drop-column column-name])

(defn alter-column
  [column-name column-type]
  [:alter-column [column-name column-type]])

(defn column-definitions
  [m]
  [:column-definitions m])

;;
;;
;;
