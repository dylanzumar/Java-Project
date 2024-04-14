Author: Dylan Zumar
Date: 1/28/23

Dependencies:
- mySQL 8.0.32
- solr 8.11
- Java 19.0.2
- univocity 2.0.0 (for parsing TSV)
- jdbc mySQL connector 8.0.32

Instructions:

1. Create user on mySQL and grant privileges to create databases and to assign permissions on databases
2. Create .sql schema
3. Assign database, table name, mySQL server endpoint, username, password, path to data, and path to schema within predictSpringUpload.java
4. Run predictSpringUpload.java in order to upload data to mySQL table
5. Create solr 8.11 instance and follow instructions here to upload mySQL table to solr: https://github.com/rohitbemax/dataimporthandler
6. Assign fields in solr cloud instance in order to query with APIs