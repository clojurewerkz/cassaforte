(ns clojurewerkz.cassaforte.cluster.client
  (:require [clojurewerkz.cassaforte.cluster.conversion :as conv])
  (:import [com.datastax.driver.core Cluster Cluster$Builder Session PreparedStatement Query
            HostDistance]))

(def ^{:dynamic true :tag Session}
  *client*)

(defn build-cluster
  [contact-points
   &{:keys [connections-per-host
           max-connections-per-host]}]
  (let [^Cluster$Builder builder (Cluster/builder)
        pooling-options (.poolingOptions builder)]
    (when connections-per-host
      (.setCoreConnectionsPerHost pooling-options HostDistance/LOCAL
                                  connections-per-host))
    (when max-connections-per-host
      (.setMaxConnectionsPerHost pooling-options HostDistance/LOCAL
                                 max-connections-per-host))
    (doseq [contact-point contact-points]
      (.addContactPoint builder contact-point))
    (.build builder)))

(defn ^Session connect
  "Connect to a Cassandra cluster"
  [hostnames]
  (.connect ^Cluster (build-cluster hostnames)))

;;
;; DB Ops
;;
(defn ^clojure.lang.IPersistentMap execute-raw
  [^Session client ^String query]
  (-> (.execute client query)
                conv/to-map))

;; TODO: separate prepare command from execute-prepared

(defn execute-prepared
  [^Session client [^String query ^java.util.List values]]
  (let [^Query prepared-statement (.bind (.prepare client query) (to-array values))]
    (-> (.execute client prepared-statement)
        conv/to-map)))

(defn get-hosts
  "Returns all hosts for connected cluster"
  [^Session client]
  (map conv/to-map (-> client
                       (.getCluster)
                       (.getMetadata)
                       (.getAllHosts))))

(defn export-schema
  "Exports schema as string"
  [^Session client]
  (-> client
      (.getCluster)
      (.getMetadata)
      (.exportSchemaAsString)))

;; Add load balancing policy
;; add compression
;; Add retry policy
