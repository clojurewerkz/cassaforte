(ns clojurewerkz.cassaforte.query.dsl
  (:import [com.datastax.driver.core.querybuilder QueryBuilder]))

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
  [:what-all nil])

(defn unix-timestamp-of
  [column-name]
  [:what-fcall ["unixTimestampOf" [(QueryBuilder/raw (name column-name))]]])

(defn date-of
  [column-name]
  [:what-fcall ["dateOf" [(QueryBuilder/raw (name column-name))]]])

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
  ([table-name]
   [:from (name table-name)])
  ([keyspace-name table-name]
   [:from [(name keyspace-name) (name table-name)]]))

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
  [& m]
  [:using (apply array-map m)])

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

(defn rename-column
  [from to]
  [:rename-column [from to]])

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
;; Index
;;

(defn on-table
  [table-name]
  [:on-table (name table-name)])
(defn and-column
  [column-name]
  [:and-column (name column-name)])
(defn and-keys-of-column
  [column-name]
  [:and-keys-of-column (name column-name)])


;;
;;
;;

(defn paginate
  "Paginate through the collection of results

   Params:
     * `where` - where query to lock a partition key
     * `key` - key to paginate on
     * `last-key` - last seen value of the key, next chunk of results will contain all keys that follow that value
     * `per-page` - how many results per page"
  ([& {:keys [key last-key per-page where] :or {:page 0}}]
   [:paginate [per-page (if last-key
                          (conj (vec where) [> key last-key])
                          where)]]))
