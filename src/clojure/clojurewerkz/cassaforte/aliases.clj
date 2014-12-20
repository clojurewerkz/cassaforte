;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

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
