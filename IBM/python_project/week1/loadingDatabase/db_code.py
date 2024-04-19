#Database initiation

import sqlite3
import pandas as pd
#Now, you can use SQLite3 to create and connect your process to a new database STAFF using the following statements.
conn = sqlite3.connect('STAFF.db')

#Create and Load the table

#To create a table in the database, you first need to have the attributes of the required table. Attributes are columns of the table. Along with their names, the knowledge of their data types are also required. The attributes for the required tables in this lab were shared in the Lab Scenario.
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

#Reading the CSV file

#Now, to read the CSV using Pandas, you use the read_csv() function. Since this CSV does not contain headers, you can use the keys of the attribute_dict dictionary as a list to assign headers to the data. For this, add the commands below to db_code.py

file_path = './week1/loadingDatabase/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

#Loading the data to a table

df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')


#Running basic queries on data

#Now that the data is uploaded to the table in the database, anyone with access to the database can retrieve this data by executing SQL queries.

#Some basic SQL queries to test this data are SELECT queries for viewing data, and COUNT query to count the number of entries.

#SQL queries can be executed on the data using the read_sql function in pandas.

#Now, run the following tasks for data retrieval on the created database.

#1.Viewing all the data in the table.
#Add the following lines of code to db_code.py

query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#2.Viewing only FNAME column of data.
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#3.Viewing the total number of entries in the table.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#Now try appending some data to the table. Consider the following.
#a. Assume the ID is 100.
#b. Assume the first name, FNAME, is John.
#c. Assume the last name as LNAME, Doe.
#d. Assume the city of residence, CITY is Paris.
#e. Assume the country code, CCODE is FR.

data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

#Now use the following statement to append the data to the INSTRUCTOR table.
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

#Now, repeat the COUNT query. You will observe an increase by 1 in the output of the first COUNT query and the second one.

#Before proceeding with the final execution, you need to add the command to close the connection to the database after all the queries are executed.

#Add the following line at the end of db_code.py to close the connection to the database.
conn.close()
