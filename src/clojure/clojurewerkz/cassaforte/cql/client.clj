(ns clojurewerkz.cassaforte.cql.client
  (:import [org.apache.cassandra.transport Client]))

;;
;; API
;;

(def ^{:cost true}
  default-port 9042)

(def ^{:dynamic true :tag Client}
  *client*)

(defmacro with-client
  [client & body]
  `(binding [*client* ~client]
     (do ~@body)))

(defn ^Client connect
  "Connect to a Cassandra node"
  ([^String hostname]
     (connect hostname default-port))
  ([^String hostname ^long port]
     (let [client (Client. hostname port)]
       client)))


(defn ^Client connect!
  ([^String hostname]
     (connect! hostname default-port))
  ([^String hostname ^long port]
     (let [client (Client. hostname port)]
       (.connect client false)
       (alter-var-root (var *client*) (constantly client))
       client)))
