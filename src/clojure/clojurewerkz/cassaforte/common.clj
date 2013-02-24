(ns clojurewerkz.cassaforte.common
  )

(defprotocol DBOperations
  (execute-raw [q])
  (execute-prepared [q]))
