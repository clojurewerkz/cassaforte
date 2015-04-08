(ns clojurewerkz.cassaforte.new-query-api-test
  (:refer-clojure :exclude [update])
  (:import [com.datastax.driver.core.utils Bytes])
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
                 (columns "first" "second"))))

  (is (= "SELECT * FROM foo WHERE foo=1 AND bar=2;"
         (select :foo
                 ;; Normally, hashmap can be used. Array map is used here to guarantee order.
                 (where (array-map :foo 1
                                   :bar 2)))))
  (is (=  "SELECT * FROM foo WHERE foo='bar' AND moo>3 AND meh>4 AND baz IN (5,6,7);"
          (select :foo
                  (where [[:= :foo "bar"]
                          [:> :moo 3]
                          [:> :meh 4]
                          [:in :baz [5 6 7]]
                          ]))))

  (is (=  "SELECT * FROM foo WHERE foo='bar' ORDER BY foo ASC;"
          (select :foo
                  (order-by (asc :foo))
                  (where [[= :foo "bar"]]))))

  (is (=  "SELECT * FROM foo WHERE foo='bar' ORDER BY foo ASC LIMIT 10;"
          (select :foo
                  (order-by (asc :foo))
                  (limit 10)
                  (where [[= :foo "bar"]]))))

  (is (=  "SELECT * FROM foo WHERE foo='bar' ORDER BY foo ASC LIMIT 10 ALLOW FILTERING;"
          (select :foo
                  (where [[= :foo "bar"]])
                  (order-by (asc :foo))
                  (limit 10)
                  (allow-filtering)
                  )))

  (is (=  "SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z';";
          (select :foo
                  (where [[= :k 4]
                          [> :c "a"]
                          [<= :c "z"]])
                  )))

  (is (= "SELECT DISTINCT asd AS bsd FROM foo;")
      (select :foo
              (columns (-> "asd"
                           distinct*
                           (as "bsd")))))

  (is (= "SELECT DISTINCT longName AS a,ttl(longName) AS ttla FROM foo LIMIT :limit;";
         (select :foo
                 (columns (-> "longName"
                              distinct*
                              (as "a"))
                          (-> "longName"
                              ttl-column
                              (as "ttla")))
                 (limit (? "limit")))))

  (is (= "SELECT a,b,\"C\" FROM foo WHERE a IN ('127.0.0.1','127.0.0.3') AND \"C\"='foo' ORDER BY a ASC,b DESC LIMIT 42;";
         (select :foo
                 (columns "a"
                          "b"
                          (quote* "C"))
                 (where [[:in :a ["127.0.0.1", "127.0.0.3"]]
                         [:= (quote* "C") "foo"]])
                 (order-by (asc :a)
                           (desc :b))
                 (limit (int 42)))))


  (is (= "SELECT writetime(a),ttl(a) FROM foo ALLOW FILTERING;"
         (select :foo
                 (columns (write-time :a)
                          (ttl-column :a))
                 (allow-filtering))))


  (is (= "SELECT DISTINCT longName AS a,ttl(longName) AS ttla FROM foo WHERE k IN () LIMIT :limit;";
         (select :foo
                 (columns (-> "longName"
                              (distinct*)
                              (as "a"))
                          (-> "longName"
                              (ttl-column)
                              (as "ttla")))
                 (where [[:in :k []]])
                 (limit (? "limit")))))

  (is (= "SELECT * FROM foo WHERE bar=:barmark AND baz=:bazmark LIMIT :limit;"
         (select :foo
                 (where [[:= :bar (? "barmark")]
                         [:= :baz (? "bazmark")]])
                 (limit (? "limit")))))

  (is (= "SELECT a FROM foo WHERE k IN ?;";
         (select :foo
                 (column :a)
                 (where [[:in :k [(?)]]]))))

  (is (= "SELECT count(*) FROM foo;";
         (select :foo
                 (count-all))))

  (is (= "SELECT intToBlob(b) FROM foo;"
         (select :foo
                 (fcall "intToBlob"
                        (cname "b")))))


  (is (= "SELECT * FROM foo WHERE k>42 LIMIT 42;";
         (select :foo
                 (where [[:> :k 42]])
                 (limit 42))))

  (is (= "SELECT * FROM foo WHERE token(k)>token(42);"
         (select :foo
                 (where [[:> (token "k") (function-call "token" 42)]]))))

  (is (= "SELECT * FROM foo2 WHERE token(a,b)>token(42,101);";
         (select :foo2
                 (where [[:> (token "a" "b") (function-call "token" 42 101)]]))))

  (is (= "SELECT * FROM words WHERE w='):,ydL ;O,D';"
         (select :words
                 (where {:w "):,ydL ;O,D"}))))

  (is (= "SELECT * FROM words WHERE w='WA(!:gS)r(UfW';"
         (select :words
                 (where {:w "WA(!:gS)r(UfW"}))))

  (is (= "SELECT * FROM foo WHERE d=1234325;"
         (select :foo
                 (where {:d (java.util.Date. 1234325)}))))

  (is (= "SELECT * FROM foo WHERE b=0xcafebabe;"
         (select :foo
                 (where {:b (Bytes/fromHexString "0xCAFEBABE")}))))

  (is (thrown? java.lang.IllegalStateException
               (select :foo
                       (count-all)
                       (order-by (asc "a") (desc "b"))
                       (order-by (asc "a") (desc "b")))))

  (is (thrown? java.lang.IllegalArgumentException
               (select :foo
                       (limit -42))))

  (is (thrown? java.lang.IllegalStateException
               (select :foo
                       (limit 42)
                       (limit 42))))

  (is (= "INSERT INTO foo(asd) VALUES ('bsd');"
         (insert :foo
                 (value "asd" "bsd"))))
  (is (= "INSERT INTO foo(asd) VALUES ('bsd');"
         (insert :foo
                 (values ["asd"] ["bsd"]))))

  (is (= "INSERT INTO foo(asd) VALUES ('bsd');"
         (insert :foo
                 (values {"asd" "bsd"}))))


  (is (= "INSERT INTO foo(a,b,\"C\",d) VALUES (123,'127.0.0.1','foo''bar',{'x':3,'y':2}) USING TIMESTAMP 42 AND TTL 24;"
         (insert :foo
                 (value "a" 123)
                 (value "b" (java.net.InetAddress/getByName "127.0.0.1"))
                 (value (quote* "C") "foo'bar")
                 (value "d" (doto (java.util.TreeMap.)
                              (.put "x" 3)
                              (.put "y" 2)))
                 (using (array-map :timestamp 42
                                   :ttl       24))

                 )
         ))
  )


;; select().countAll().from("foo").orderBy(asc("a"), desc("b")).orderBy(asc("a"), desc("b"));
;; select().column("a").all().from("foo");
;; select().column("a").countAll().from("foo");
;; select().all().from("foo").limit(-42);
;; select().all().from("foo").limit(42).limit(42);
