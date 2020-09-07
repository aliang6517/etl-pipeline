#Sparkify ETL using postgresql\n

##Getting started \n
Use create_tables.py to create the tables and drop the ones that already exist\n under the same name. The queries execute by the script are written in postgresql and\n contained in the sql_queries.py file.


##Running the tests\n
###Each test runs a query to return the first 5 records of each table \n
Select the first 5 records from each table to ensure that records were correctly\n
 loaded into tables.\n
'''
%sql SELECT * FROM songplays LIMIT 5;
%sql SELECT * FROM users LIMIT 5;
'''

##Built with\n
[Postgresql](https://www.postgresql.org/docs/) - SQL database used\n
[Python](https://www.python.org/) - ETL scripts
