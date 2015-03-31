(ns clojurewerkz.cassaforte.new-query-api-test
  (:refer-clojure :exclude [update])
  (:require [clojure.test                          :refer :all]
            [clojurewerkz.cassaforte.new-query-api :refer :all]
            ))

(deftest test-select-query
  (is (= "SELECT asd FROM \"table-name\";"
         (select "table-name"
                 (column "asd"))))
  (is (= "SELECT first,second FROM \"table-name\";"
         (select "table-name"
                 (column "first")
                 (column "second")
                 )))
  (is (= "SELECT first,second FROM \"table-name\";"
         (select "table-name"
                 (columns ["first" "second"]))))
  )
