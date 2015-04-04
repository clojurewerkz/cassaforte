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
                 (columns "first" "second"))))

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
                              ttl
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



  )


;; "SELECT writetime(a),ttl(a) FROM foo ALLOW FILTERING;";
;; select().writeTime("a").ttl("a").from("foo").allowFiltering();



;; "SELECT DISTINCT longName AS a,ttl(longName) AS ttla FROM foo WHERE k IN () LIMIT :limit;";
;; select().distinct().column("longName").as("a").ttl("longName").as("ttla").from("foo").where(in("k")).limit(bindMarker("limit"));

;; "SELECT * FROM foo WHERE bar=:barmark AND baz=:bazmark LIMIT :limit;";
;; select().all().from("foo").where().and(eq("bar", bindMarker("barmark"))).and(eq("baz", bindMarker("bazmark"))).limit(bindMarker("limit"));

;; "SELECT a FROM foo WHERE k IN ();";
;; select("a").from("foo").where(in("k"));

;; "SELECT a FROM foo WHERE k IN ?;";
;; select("a").from("foo").where(in("k", bindMarker()));

;; "SELECT count(*) FROM foo;";
;; select().countAll().from("foo");

;; "SELECT intToBlob(b) FROM foo;";
;; select().fcall("intToBlob", column("b")).from("foo");

;; "SELECT * FROM foo WHERE k>42 LIMIT 42;";
;; select().all().from("foo").where(gt("k", 42)).limit(42);

;; "SELECT * FROM foo WHERE token(k)>token(42);";
;; select().all().from("foo").where(gt(token("k"), fcall("token", 42)));

;; "SELECT * FROM foo2 WHERE token(a,b)>token(42,101);";
;; select().all().from("foo2").where(gt(token("a", "b"), fcall("token", 42, 101)));

;; "SELECT * FROM words WHERE w='):,ydL ;O,D';";
;; select().all().from("words").where(eq("w", "):,ydL ;O,D"));

;; "SELECT * FROM words WHERE w='WA(!:gS)r(UfW';";
;; select().all().from("words").where(eq("w", "WA(!:gS)r(UfW"));

;; Date date = new Date();
;; date.setTime(1234325);
;; "SELECT * FROM foo where d=1234325";
;; select().all().from("foo").where(eq("d", date));

;; "SELECT * FROM foo where b=0xCAFEBABE";
;; select().all().from("foo").where(eq("b", Bytes.fromHexString("0xCAFEBABE")));

;; select().countAll().from("foo").orderBy(asc("a"), desc("b")).orderBy(asc("a"), desc("b"));
;; select().column("a").all().from("foo");
;; select().column("a").countAll().from("foo");
;; select().all().from("foo").limit(-42);
;; select().all().from("foo").limit(42).limit(42);
