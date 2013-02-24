(ns clojurewerkz.cassaforte.simple.client
  (:require [clojurewerkz.cassaforte.conversion :as conv]
            [clojurewerkz.cassaforte.bytes :as cb])
  (:import [org.apache.cassandra.transport Client]))

;;
;; API
;;

(def ^{:cost true}
  default-port 9042)

(defn ^Client connect
  "Connect to a Cassandra node"
  ([^String hostname]
     (connect hostname default-port))
  ([^String hostname ^long port]
     (let [client (Client. hostname port)]
       (.connect client false)
       client)))

;;
;; DB Ops
;;

(def ^:dynamic *default-consistency-level* org.apache.cassandra.db.ConsistencyLevel/ONE)
(def prepared-statements-cache (atom {}))

(defn prepare-cql-query
  "Prepares a CQL query for execution."
  [client ^String query]
  (if-let [statement-id (get @prepared-statements-cache query)]
    statement-id
    (let [statement-id (.bytes (.statementId (.prepare ^Client client query)))]
      (swap! prepared-statements-cache assoc query statement-id)
      statement-id)))

(defn execute-prepared
  "Executes a CQL query previously prepared using the prepare-cql-query function
   by providing the actual values for placeholders"
  ([client q]
     (execute-prepared client q *default-consistency-level*))
  ([client [^String query ^java.util.List values] consistency-level]
     (conv/to-plain-hash (:rows
                          (conv/to-map (.toThriftResult
                                        (.executePrepared ^Client client
                                                          (prepare-cql-query ^Client client query)
                                                          (map cb/encode values)
                                                          consistency-level)))))))


(defn ^clojure.lang.IPersistentMap execute-raw
  ([^Client client query]
     (execute-raw client query *default-consistency-level*))
  ([^Client client ^String query consistency-level]
     (-> (.execute ^Client client query consistency-level)
         (.toThriftResult)
         conv/to-map
         (:rows)
         conv/to-plain-hash)))
