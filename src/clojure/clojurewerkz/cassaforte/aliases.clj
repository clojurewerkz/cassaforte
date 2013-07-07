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
