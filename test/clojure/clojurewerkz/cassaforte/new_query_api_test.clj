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

  (is (=  "SELECT * FROM foo WHERE foo='bar' AND moo>3 AND meh>4 AND baz IN (5,6,7);"
          (select :foo
                  (all)
                  (where [[= :foo "bar"]
                          [> :moo 3]
                          [:> :meh 4]
                          [:in :baz [5 6 7]]
                          ]))))

  (is (=  "SELECT * FROM foo WHERE foo='bar' ORDER BY foo ASC;"
          (select :foo
                  (all)
                  (order-by [(asc :foo)])
                  (where [[= :foo "bar"]]))))

  (is (=  "SELECT * FROM foo WHERE foo='bar' ORDER BY foo ASC LIMIT 10;"
          (select :foo
                  (all)
                  (order-by [(asc :foo)])
                  (limit 10)
                  (where [[= :foo "bar"]]))))

  (is (=  "SELECT * FROM foo WHERE foo='bar' ORDER BY foo ASC LIMIT 10 ALLOW FILTERING;"
          (select :foo
                  (all)
                  (where [[= :foo "bar"]])
                  (order-by [(asc :foo)])
                  (limit 10)
                  (allow-filtering)
                  )))
  )
