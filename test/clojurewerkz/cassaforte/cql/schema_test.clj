(ns clojurewerkz.cassaforte.cql.schema-test
  (:require [clojurewerkz.cassaforte.cql.client :as cc]
            [clojurewerkz.cassaforte.cql.schema :as cql-schema]
            [clojurewerkz.cassaforte.cql    :as cql]
            [clojurewerkz.cassaforte.ddl.column-family-definition :as cfd])
  (:use clojure.test
        clojurewerkz.cassaforte.test-helper
        clojurewerkz.cassaforte.conversion
        clojurewerkz.cassaforte.utils))

(use-fixtures :once initialize-cql)

(deftest ^{:schema true :indexes true} test-create-columnfamily-bare-cql
  (with-native-exception-handling
    (cql-schema/drop-column-family "libraries"))

  (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                                   language varchar,
                                                  PRIMARY KEY (name))")
  (is (cql/void-result? (cql-schema/create-index "libraries" "language")))

  (is (cql/void-result? (cql-schema/drop-index "libraries" "language")))

  (is (cql/void-result? (cql-schema/create-index "libraries" "language")))
  (is (cql/void-result? (cql-schema/drop-index "libraries_language_idx")))
  (cql-schema/drop-column-family "libraries"))


(deftest ^{:schema true :cql true} test-create-describe
  (with-native-exception-handling
    (cql-schema/drop-column-family "posts"))
  (cql-schema/create-column-family "posts"
                            {:userid :text
                             :posted_at :timestamp
                             :entry_title :text
                             :content :text}
                            :primary-key [:userid :posted_at])

  (let [desciption (cql-schema/describe "cassaforte_test_1" "posts" :with-columns true)]
    (is (= "posts" (:columnfamily_name desciption)))
    (is (= 2 (count (:columns desciption))))))
