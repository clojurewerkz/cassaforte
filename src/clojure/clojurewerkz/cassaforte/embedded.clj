;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns clojurewerkz.cassaforte.embedded
  "Provides facility functions for working with an embedded Cassandra server, which is very useful
   for testing your application without having a C* cluster running and for cases when application
   requires Standalone Cassandra without additional installation.

   In order to work with an embedded cluster, you'll have to have `cassandra-all` dependency in your
   project.clj, for example:

      [org.apache.cassandra/cassandra-all \"2.0.2\"]"
  (:require [clojure.java.io :as io])
  (:import [org.apache.cassandra.service CassandraDaemon]
           [org.apache.cassandra.config DatabaseDescriptor]))

(declare daemon)

(defn- delete-file
  "Delete file f. Raise an exception if it fails unless silently is true."
  [f & [silently]]
  (or (.delete (io/file f))
      silently
      (throw (java.io.IOException. (str "Couldn't delete " f)))))

(defn- delete-file-recursively
  "Delete file f. If it's a directory, recursively delete all its contents.
Raise an exception if any deletion fails unless silently is true."
  [f & [silently]]
  (let [f (io/file f)]
    (if (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-file-recursively child silently)))
    (delete-file f silently)))

(defn start-server!
  "Starts embedded server using configuration file located under `resources/cassandra.yaml`.

   Keys:
     * cleanup: wether or not remove previously existing cluster files. Cleanup is especially useful when
       embedded server is used for test purposes. Break your server, wipe it out and start breaking it again."
  [& {:keys [cleanup] :or {:cleanup true}}]
  (System/setProperty "cassandra.config" (str (io/resource "cassandra20.yaml")))
  ;; If you're running Cassandra on Mac Os X on 1.7, you'll get in trouble with parsing version, because
  ;; "1.7.0_06-ea" ea doesn't parse as int. That's a workaround
  (System/setProperty "java.version" "1.7.0_15")
  (System/setProperty "cassandra-foreground" "yes")
  (System/setProperty "log4j.defaultInitOverride" "false")
  (System/setProperty "log4j.appender.R.File" "/var/log/cassandra/system.log")

  (when-not (bound? (var daemon))
    (if (and cleanup
             (.exists (io/file "tmp")))
      (delete-file-recursively "tmp"))
    (def daemon (let [d (CassandraDaemon.)]
                  (.init d nil)
                  (.start d)
                  d))))


(defn stop-server!
  "Stops started embedded server"
  []
  (.deactivate ^CassandraDaemon daemon)
  (.unbindRoot #'daemon))
