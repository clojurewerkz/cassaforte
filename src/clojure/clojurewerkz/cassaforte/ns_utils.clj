(ns clojurewerkz.cassaforte.ns-utils)

(defmacro defalias ^:private [sym v]
  `(intern *ns* (with-meta ~sym (meta ~sym)) (deref ~v)))

(defn alias-ns
  "Alias all the vars from namespace to the curent namespace"
  [ns-name]
  (require ns-name)
  (doseq [[n v] (ns-publics (the-ns ns-name))]
    (defalias n v)))
