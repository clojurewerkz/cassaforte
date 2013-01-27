(ns clojurewerkz.cassaforte.schema-test
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql]
            [clojurewerkz.cassaforte.ddl.column-family-definition :as cfd])
  (:use clojure.test
        clojurewerkz.cassaforte.test-helper
        clojurewerkz.cassaforte.conversion
        clojurewerkz.cassaforte.utils))

(use-fixtures :once initialize-cql)

(deftest ^{:schema true :indexes true} test-create-columnfamily-bare-cql
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries")
    (cql/execute-raw "CREATE COLUMNFAMILY libraries (name     varchar,
                                                   language varchar,
                                                  PRIMARY KEY (name))")
    (is (cql/void-result? (sch/create-index "libraries" "language")))
    (is (cql/void-result? (sch/drop-index "libraries" "language")))
    (is (cql/void-result? (sch/create-index "libraries" "language" "by_language")))
    (is (cql/void-result? (sch/drop-index "by_language")))
    (cql/drop-column-family "libraries")))
