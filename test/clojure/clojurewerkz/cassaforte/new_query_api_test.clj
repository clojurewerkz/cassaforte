(ns clojurewerkz.cassaforte.new-query-api-test
  (:import [com.datastax.driver.core.utils Bytes])
  (:require [clojure.test                          :refer :all]
            [clojurewerkz.cassaforte.new-query-api :refer :all]))

(defn normalize-string
  [s]
  (clojure.string/replace s "\t" "  "))

(deftest test-select-query

  (is (= "SELECT asd FROM \"table-name\";"
         (normalize-string
          (select "table-name"
                  (column "asd")))))

  (is (= "SELECT first,second FROM \"table-name\";"
         (normalize-string
          (select "table-name"
                  (column "first")
                  (column "second")))))

  (is (= "SELECT first,second FROM \"table-name\";"
         (normalize-string
          (select "table-name"
                  (columns "first" "second")))))

  (is (= "SELECT * FROM foo WHERE foo=1 AND bar=2;"
         (normalize-string
          (select :foo
                  ;; Normally, hashmap can be used. Array map is used here to guarantee order.
                  (where (array-map :foo 1
                                    :bar 2))))))
  (is (=  "SELECT * FROM foo WHERE foo='bar' AND moo>3 AND meh>4 AND baz IN (5,6,7);"
          (normalize-string
           (select :foo
                   (where [[:= :foo "bar"]
                           [:> :moo 3]
                           [:> :meh 4]
                           [:in :baz [5 6 7]]
                           ])))))

  (is (=  "SELECT * FROM foo WHERE foo='bar' ORDER BY foo ASC;"
          (normalize-string
           (select :foo
                   (order-by (asc :foo))
                   (where [[= :foo "bar"]])))))

  (is (=  "SELECT * FROM foo WHERE foo='bar' ORDER BY foo ASC LIMIT 10;"
          (normalize-string
           (select :foo
                   (order-by (asc :foo))
                   (limit 10)
                   (where [[= :foo "bar"]])))))

  (is (=  "SELECT * FROM foo WHERE foo='bar' ORDER BY foo ASC LIMIT 10 ALLOW FILTERING;"
          (normalize-string
           (select :foo
                   (where [[= :foo "bar"]])
                   (order-by (asc :foo))
                   (limit 10)
                   (allow-filtering)
                   ))))

  (is (=  "SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z';";
          (normalize-string
           (select :foo
                   (where [[= :k 4]
                           [> :c "a"]
                           [<= :c "z"]])
                   ))))

  (is (= "SELECT DISTINCT asd AS bsd FROM foo;"
         (normalize-string
          (select :foo
                  (columns (-> "asd"
                               distinct*
                               (as "bsd")))))))

  (is (= "SELECT DISTINCT longName AS a,ttl(longName) AS ttla FROM foo LIMIT :limit;";
         (normalize-string
          (select :foo
                  (columns (-> "longName"
                               distinct*
                               (as "a"))
                           (-> "longName"
                               ttl-column
                               (as "ttla")))
                  (limit (bind-marker "limit"))))))

  (is (= "SELECT a,b,\"C\" FROM foo WHERE a IN ('127.0.0.1','127.0.0.3') AND \"C\"='foo' ORDER BY a ASC,b DESC LIMIT 42;";
         (normalize-string
          (select :foo
                  (columns "a"
                           "b"
                           (quote* "C"))
                  (where [[:in :a ["127.0.0.1", "127.0.0.3"]]
                          [:= (quote* "C") "foo"]])
                  (order-by (asc :a)
                            (desc :b))
                  (limit (int 42))))))


  (is (= "SELECT writetime(a),ttl(a) FROM foo ALLOW FILTERING;"
         (normalize-string
          (select :foo
                  (columns (write-time :a)
                           (ttl-column :a))
                  (allow-filtering)))))


  (is (= "SELECT DISTINCT longName AS a,ttl(longName) AS ttla FROM foo WHERE k IN () LIMIT :limit;";
         (normalize-string
          (select :foo
                  (columns (-> "longName"
                               (distinct*)
                               (as "a"))
                           (-> "longName"
                               (ttl-column)
                               (as "ttla")))
                  (where [[:in :k []]])
                  (limit (bind-marker "limit"))))))

  (is (= "SELECT * FROM foo WHERE bar=:barmark AND baz=:bazmark LIMIT :limit;"
         (normalize-string
          (select :foo
                  (where [[:= :bar (bind-marker "barmark")]
                          [:= :baz (bind-marker "bazmark")]])
                  (limit (bind-marker "limit"))))))

  (is (= "SELECT a FROM foo WHERE k IN ?;";
         (normalize-string
          (select :foo
                  (column :a)
                  (where [[:in :k [?]]])))))

  (is (= "SELECT count(*) FROM foo;";
         (normalize-string
          (select :foo
                  (count-all)))))

  (is (= "SELECT intToBlob(b) FROM foo;"
         (normalize-string
          (select :foo
                  (fcall "intToBlob"
                         (cname "b"))))))


  (is (= "SELECT * FROM foo WHERE k>42 LIMIT 42;";
         (normalize-string
          (select :foo
                  (where [[:> :k 42]])
                  (limit 42)))))

  (is (= "SELECT * FROM foo WHERE token(k)>token(42);"
         (normalize-string
          (select :foo
                  (where [[:> (token "k") (function-call "token" 42)]])))))

  (is (= "SELECT * FROM foo2 WHERE token(a,b)>token(42,101);";
         (normalize-string
          (select :foo2
                  (where [[:> (token "a" "b") (function-call "token" 42 101)]])))))

  (is (= "SELECT * FROM words WHERE w='):,ydL ;O,D';"
         (normalize-string
          (select :words
                  (where {:w "):,ydL ;O,D"})))))

  (is (= "SELECT * FROM words WHERE w='WA(!:gS)r(UfW';"
         (normalize-string
          (select :words
                  (where {:w "WA(!:gS)r(UfW"})))))

  (is (= "SELECT * FROM foo WHERE d=1234325;"
         (normalize-string
          (select :foo
                  (where {:d (java.util.Date. 1234325)})))))

  (is (= "SELECT * FROM foo WHERE b=0xcafebabe;"
         (normalize-string
          (select :foo
                  (where {:b (Bytes/fromHexString "0xCAFEBABE")})))))

  (is (thrown? java.lang.IllegalStateException
               (normalize-string
                (select :foo
                        (count-all)
                        (order-by (asc "a") (desc "b"))
                        (order-by (asc "a") (desc "b"))))))

  (is (thrown? java.lang.IllegalArgumentException
               (normalize-string
                (select :foo
                        (limit -42)))))

  (is (thrown? java.lang.IllegalStateException
               (normalize-string
                (select :foo
                        (limit 42)
                        (limit 42)))))

  (is (= "INSERT INTO foo(asd) VALUES ('bsd');"
         (normalize-string
          (insert :foo
                  {"asd" "bsd"}))))
  (is (= "INSERT INTO foo(asd) VALUES ('bsd');"
         (normalize-string
          (insert :foo
                  {"asd" "bsd"}))))

  (is (= "INSERT INTO foo(asd) VALUES ('bsd');"
         (normalize-string
          (insert :foo
                  {"asd" "bsd"}))))


  (is (= "INSERT INTO foo(a,b,\"C\",d) VALUES (123,'127.0.0.1','foo''bar',{'x':3,'y':2}) USING TIMESTAMP 42 AND TTL 24;"
         (normalize-string
          (insert :foo
                  (array-map "a"          123
                             "b"          (java.net.InetAddress/getByName "127.0.0.1")
                             (quote* "C") "foo'bar"
                             "d"          (array-map "x" 3 "y" 2))
                  (using (array-map :timestamp 42
                                    :ttl       24))))))

  (is (= "INSERT INTO foo(a,b,\"C\",d) VALUES (123,'127.0.0.1','foo''bar',{'x':3,'y':2}) USING TIMESTAMP 42 AND TTL 24;"
         (normalize-string
          (insert :foo
                  (array-map "a"          123
                             "b"          (java.net.InetAddress/getByName "127.0.0.1")
                             (quote* "C") "foo'bar"
                             "d"          {"x" 3 "y" 2})
                  (using (array-map :timestamp 42
                                    :ttl       24))))))

  (is (= "INSERT INTO foo(a,b) VALUES ({2,3,4},3.4) USING TIMESTAMP 42 AND TTL 24;"
         (normalize-string
          (insert :foo
                  (array-map "a" (sorted-set 2 3 4)
                             "b" 3.4)
                  (using (array-map :timestamp 42
                                    :ttl       24))))))

  (is (= "INSERT INTO foo(a,b) VALUES ({2,3,4},3.4) USING TTL ? AND TIMESTAMP ?;"
         (normalize-string
          (insert :foo
                  (array-map :a (sorted-set 2 3 4)
                             :b 3.4)
                  (using (array-map :ttl       ?
                                    :timestamp ?))))))

  (is (= "INSERT INTO foo(c,a,b) VALUES (123,{2,3,4},3.4) USING TIMESTAMP 42;"
         (normalize-string
          (insert :foo
                  (array-map :c 123
                             :a (sorted-set 2 3 4)
                             :b 3.4)
                  (using {:timestamp 42})))))

  (is (= "INSERT INTO foo(k,x) VALUES (0,1) IF NOT EXISTS;";
         (normalize-string
          (insert :foo
                  (array-map :k 0
                             :x 1)
                  (if-not-exists)))))


  (is (= "INSERT INTO foo(k,x) VALUES (0,(1));";
         (normalize-string
          (insert :foo
                  (array-map :k 0
                             :x (tuple-of [:int] [(int 1)]))))))

  (is (= "UPDATE foo USING TIMESTAMP 42 SET a=12,b=[3,2,1],c=c+3 WHERE k=2;"
         (normalize-string
          (update :foo
                  (array-map :a 12
                             :b [3 2 1]
                             :c (increment-by 3))
                  (where {:k 2})
                  (using {:timestamp 42})))))


  (is (= "UPDATE foo SET b=null WHERE k=2;"
         (normalize-string
          (update :foo
                  (array-map :b nil)
                  (where {:k 2})))))


  (is (= "UPDATE foo SET a[2]='foo',b=[3,2,1]+b,c=c-{'a'} WHERE k=2 AND l='foo' AND m<4 AND n>=1;"
         (normalize-string
          (update :foo
                  (array-map :a (set-idx 2 "foo")
                             :b (prepend-all [3,2,1])
                             :c (remove-tail "a"))
                  (where [[= :k 2]
                          [= :l "foo"]
                          [< :m 4]
                          [>= :n 1]])
                  ))))


  (is (= "UPDATE foo SET b=[3]+b,c=c+['a'],d=d+[1,2,3],e=e-[1];"
         (normalize-string
          (update :foo
                  (array-map :b (prepend 3)
                             :c (append "a")
                             :d (append-all [1 2 3])
                             :e (discard 1))))))

  (is (= "UPDATE foo SET b=b-[1,2,3],c=c+{1},d=d+{4,3,2};"
         (normalize-string
          (update :foo
                  (array-map :b (discard-all [1 2 3])
                             :c (add-tail 1)
                             :d (add-all-tail #{2 3 4}))))))

  (is (= "UPDATE foo SET b=b-{2,3,4},c['k']='v',d=d+{'x':3,'y':2};"
         (normalize-string
          (update :foo
                  (array-map :b (remove-all-tail (sorted-set 2 3 4))
                             :c (put-value "k" "v")
                             :d (put-values (array-map "x" 3
                                                       "y" 2)))))))

  (is (= "UPDATE foo USING TTL 400;"
         (normalize-string
          (update :foo
                  {}
                  (using {:ttl 400})))))


  (is (= (str "UPDATE foo SET a=" (BigDecimal. 3.2) ",b=42 WHERE k=2;")
         (normalize-string
          (update :foo
                  (array-map :a (BigDecimal. 3.2)
                             :b (BigInteger. "42"))
                  (where {:k 2})))))

  (is (= "UPDATE foo USING TIMESTAMP 42 SET b=[3,2,1]+b WHERE k=2 AND l='foo';"
         (normalize-string
          (update :foo
                  (array-map :b (prepend-all [3 2 1]))
                  (where (array-map :k 2
                                    :l "foo"))
                  (using {:timestamp 42})))))

  (is (thrown? IllegalArgumentException
               (normalize-string
                (update :foo
                        {}
                        (using {:ttl -400})))))

  (is (= "UPDATE foo SET x=4 WHERE k=0 IF x=1;"
         (normalize-string
          (update :foo
                  {:x 4}
                  (where {:k 0})
                  (only-if {:x 1})))))

  (is (= "UPDATE foo SET x=4 WHERE k=0 IF foo='bar' AND moo>3 AND meh>4 AND baz IN (5,6,7);"
         (normalize-string
          (update :foo
                  {:x 4}
                  (where {:k 0})
                  (only-if [[:= :foo "bar"]
                            [:> :moo 3]
                            [:> :meh 4]
                            [:in :baz [5 6 7]]
                            ])))))

  (is (= "DELETE a,b,c FROM foo;"
         (normalize-string
          (delete :foo
                  (columns :a :b :c)))))

  (is (= "DELETE a,b,c FROM foo WHERE k=0;"
         (normalize-string
          (delete :foo
                  (columns :a :b :c)
                  (where {:k 0})
                  ))))

  (is (= "DELETE a[3],b['foo'],c FROM foo WHERE k=1;"
         (normalize-string
          (delete :foo
                  (columns (list-elt :a 3)
                           (map-elt :b "foo")
                           :c)
                  (where {:k 1})))))

  (is (= "DELETE FROM foo USING TIMESTAMP 1240003134 WHERE k='value';"
         (normalize-string
          (delete :foo
                  (using {:timestamp 1240003134})
                  (where {:k "value"})))))

  (is (= "DELETE FROM foo WHERE k1='foo' IF EXISTS;"
         (normalize-string
          (delete :foo
                  (where {:k1 "foo"})
                  (if-exists)))))

  (is (= "DELETE FROM foo WHERE k1='foo' IF a=1 AND b=2;"
         (normalize-string
          (delete :foo
                  (where {:k1 "foo"})
                  (only-if (array-map :a 1
                                      :b 2))))))

  (is (= "DELETE FROM foo WHERE k1=:key;"
         (normalize-string
          (delete :foo
                  (where {:k1 (bind-marker "key")})))))


  (is (= "TRUNCATE a;"
         (normalize-string
          (truncate :a))))

  (is (= "TRUNCATE b.a;"
         (normalize-string
          (truncate :a :b))))

  (is (= (str "BEGIN BATCH INSERT INTO foo(asd) VALUES ('bsd');"
              "INSERT INTO foo(asd) VALUES ('bsd');"
              "INSERT INTO foo(asd) VALUES ('bsd');"
              "APPLY BATCH;")
         (batch
          (queries
           (insert :foo
                   {"asd" "bsd"})
           (insert :foo
                   {"asd" "bsd"})
           (insert :foo
                   {"asd" "bsd"})))))

  (is (= (str "BEGIN COUNTER BATCH USING TIMESTAMP 42 "
              "UPDATE foo SET a=a+1;"
              "UPDATE foo SET b=b+1;"
              "UPDATE foo SET c=c+1;"
              "APPLY BATCH;")
         (normalize-string
          (batch
           (using {:timestamp 42})
           (queries
            (update :foo
                    (array-map :a (increment-by 1)))
            (update :foo
                    (array-map :b (increment-by 1)))
            (update :foo
                    (array-map :c (increment-by 1))))))))


  (is (= "
  CREATE TABLE foo(
    a int,
    b varchar,
    c int,
    d int,
    e int,
    PRIMARY KEY((a, b), c, d))"
         (normalize-string
          (create-table :foo
                        (column-definitions (array-map :a :int
                                                       :b :varchar
                                                       :c :int
                                                       :d :int
                                                       :e :int
                                                       :primary-key [[:a :b] :c :d]))))))

  (is (= "
  CREATE TABLE foo(
    a int,
    c int,
    d int,
    b varchar,
    e int,
    PRIMARY KEY(a, c, d))"
         (normalize-string
          (create-table :foo
                        (column-definitions (array-map :a :int
                                                       :c :int
                                                       :d :int
                                                       :b :varchar
                                                       :e :int
                                                       :primary-key [:a :c :d]))))))


  (is (= "
  CREATE TABLE foo(
    a varchar,
    b varchar,
    c varchar,
    d varchar,
    PRIMARY KEY((a, b), c, d))
  WITH COMPACT STORAGE"
         (normalize-string
          (create-table :foo
                        (column-definitions (array-map :a :varchar
                                                       :b :varchar
                                                       :c :varchar
                                                       :d :varchar
                                                       :primary-key [[:a :b] :c :d]))
                        (with {:compact-storage true})
                        ))))

  (is (= "
  CREATE TABLE foo(
    a int,
    b map<varchar, varchar>,
    PRIMARY KEY(a))"
         (normalize-string
          (create-table :foo
                        (column-definitions (array-map :a :int
                                                       :b (map-type :varchar :varchar)
                                                       :primary-key [:a]))))))

  (is (= "
  CREATE TABLE IF NOT EXISTS foo(
    a int,
    b varchar,
    PRIMARY KEY(a))"
         (normalize-string
          (create-table :foo
                        (column-definitions (array-map :a :int
                                                       :b :varchar
                                                       :primary-key [:a]))
                        (if-not-exists)
                        ))))

  (is (= "
  ALTER TABLE foo ALTER foo TYPE int"
         (normalize-string
          (alter-table :foo
                       (alter-column :foo :int)))))

  (is (= "
  ALTER TABLE foo ADD bar varchar"
         (normalize-string
          (alter-table :foo
                       (add-column :bar :varchar)))))

  (is (= "
  ALTER TABLE foo DROP bar"
         (normalize-string
          (alter-table :foo
                       (drop-column :bar)))))

  (is (= "
  ALTER TABLE foo ALTER foo TYPE int"
         (normalize-string
          (alter-table :foo
                       (alter-column :foo :int)
                       (with {:default-ttl 100})
                       ))))

  (is (= "DROP TABLE foo"
         (drop-table :foo)))

  (is (= "
  CREATE INDEX foo ON bar(baz)"
         (normalize-string
          (create-index "foo"
                        (on-table "bar")
                        (and-column "baz")))))

  (is (= "
  CREATE INDEX foo ON bar(KEYS(baz))"
         (normalize-string
          (create-index "foo"
                        (on-table "bar")
                        (and-keys-of-column "baz")))))


  (is (= "DROP KEYSPACE foo;"
         (drop-keyspace "foo")))
  (is (= "DROP KEYSPACE IF EXISTS foo;"
         (drop-keyspace "foo" (if-exists))))


  (is (= "CREATE KEYSPACE IF NOT EXISTS foo;"
         (create-keyspace "foo"
                          (if-not-exists))))

  (is (= "CREATE KEYSPACE IF NOT EXISTS foo WITH REPLICATION = {class: 'SimpleStrategy', replication_factor: 1};"
         (create-keyspace "foo"
                          (with
                            {:replication
                             {"class"              "SimpleStrategy"
                              "replication_factor" 1}})
                          (if-not-exists))))

  )
