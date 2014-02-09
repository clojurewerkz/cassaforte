;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns clojurewerkz.cassaforte.aliases)

(defn alias-var
  "Create a var with the supplied name in the current namespace, having the same
  metadata and root-binding as the supplied var."
  [name ^clojure.lang.Var var]
  (apply intern *ns* (with-meta name (merge (meta var)
                                            (meta name)))
         (when (.hasRoot var) [@var])))

(defmacro defalias
  [dst src]
  `(alias-var (quote ~dst) (var ~src)))

(defn alias-ns
  "Alias all the vars from namespace to the curent namespace"
  [ns-name]
  (require ns-name)
  (doseq [[n v] (ns-publics (the-ns ns-name))]
    (alias-var n v)))
