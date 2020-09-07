#Sparkify ETL using postgresql

##Getting started

Use create_tables.py to create the tables and drop the ones that already exist
under the same name. The queries execute by the script are written in postgresql and
contained in the sql_queries.py file.


##Running the tests
###Each test runs a query to return the first 5 records of each table
Select the first 5 records from each table to ensure that records were correctly
 loaded into tables.

'''

%sql SELECT * FROM songplays LIMIT 5;
%sql SELECT * FROM users LIMIT 5;

'''

##Built with
[Postgresql](https://www.postgresql.org/docs/) - SQL database used

[Python](https://www.python.org/) - ETL scripts
