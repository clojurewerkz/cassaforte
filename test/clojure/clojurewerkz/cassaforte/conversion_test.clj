(ns clojurewerkz.cassaforte.conversion-test
  (:import [java.util HashMap ArrayList HashSet])
  (:require [clojurewerkz.cassaforte.conversion :refer :all]
            [clojure.test :refer :all]))

(deftest conversion-test
  (testing "Map Conversion"
    (let [java-map (HashMap. )]
      (doto java-map
        (.put "a" 1)
        (.put "b" 2)
        (.put "c" 3))
      (is (= {"a" 1
              "b" 2
              "c" 3}
             (to-clj java-map)))))

  (testing "Empty Map Conversion"
    (let [java-map (HashMap. )]
      (is (= {}
             (to-clj java-map)))))

  (testing "List Conversion"
    (let [java-list (ArrayList. )]
      (doto java-list
        (.add "a")
        (.add "b")
        (.add "c"))
      (is (= ["a" "b" "c"]
             (to-clj java-list)))))

  (testing "Empty List Conversion"
    (let [java-list (ArrayList. )]
      (is (= []
             (to-clj java-list)))))

    (testing "List Conversion"
    (let [java-list (HashSet. )]
      (doto java-list
        (.add "a")
        (.add "b")
        (.add "c"))
      (is (= #{"a" "b" "c"}
             (to-clj java-list)))))

  (testing "Empty List Conversion"
    (let [java-list (HashSet. )]
      (is (= #{}
             (to-clj java-list))))))
