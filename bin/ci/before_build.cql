CREATE KEYSPACE cassaforte_test_thrift WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE cassaforte_test_thrift;

CREATE KEYSPACE cassaforte_test_1;
USE cassaforte_test_1;

CREATE COLUMN FAMILY movies;
CREATE COLUMN FAMILY web_pages;
