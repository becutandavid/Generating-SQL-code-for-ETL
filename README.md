
<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
* [Getting Started](#getting-started)
  * [Installation](#installation)
* [Contact](#contact)
* [Acknowledgements](#acknowledgements)



<!-- ABOUT THE PROJECT -->
## About The Project
This project came from the desire to try and automate the process of creating dimension tables for a warehouse.

We made a python package that connects to a postgres database, and given a csv of dimension tables and columns it generates procedures for creating and maintaining the dimension tables.

We create a warehouse object that creates all the dimension tables, and has a method that writes the needed sql to a file.
The warehouse can submit the generated sql to the database, or it can save it to a file so that the user can inspect it and make modifications.


If we don't want to connect the warehouse to our database, we can generate a metadata table and export it to csv, and pass the metadata csv to the warehouse. An example of the sql code to generate this metadata is located in the folder sql_scripts

<!-- GETTING STARTED -->
## Getting Started
Create a csv in the following format:

```dim_name,table_schema,table_name,column_name```

pass a database.ini file:
```
[postgresql]
host=localhost
database=postgres
user=postgres
password=password
```


### Installation
1. Clone the repo
```sh
git clone https://github.com/becutandavid/Generating-SQL-code-for-ETL
```
2. ```from Warehouse.Warehouse import Warehouse```
3.  Create a warehouse object. 
4. Call etl()

<!-- CONTACT -->
## Contact

David Galevski - davidgalevski@gmail.com

Jovan Velkovski - jovanvelkovski2@gmail.com

<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements
* [Pandas](https://pandas.pydata.org/) 
* [psycopg2](https://pypi.org/project/psycopg2/)

