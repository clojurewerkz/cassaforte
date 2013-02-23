(ns clojurewerkz.cassaforte.cluster.client
  (:import [com.datastax.driver.core Cluster Session]))

(def ^{:dynamic true :tag Session}
  *client*)

(defn build-cluster
  [contact-points]
  (let [builder (Cluster/builder)]
    (doseq [contact-point contact-points]
      (.addContactPoint builder contact-point))
    (.build builder)))

(defmacro with-client
  [client & body]
  `(binding [*client* ~client]
     (do ~@body)))

(defn ^Session connect
  "Connect to a Cassandra cluster"
  [hostnames]
  (.connect (build-cluster hostnames)))

(defn ^Session connect!
  "Connect to a Cassandra cluster"
  [hostnames]
  (let [session (connect hostnames)]
    (alter-var-root (var *client*) (constantly session))
    session))
