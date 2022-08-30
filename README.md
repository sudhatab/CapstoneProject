# CapstoneProject
   # Project Title

This Capstone Project requires learners to work with the following technologies to manage an ETL process for a Loan Application dataset and a Credit Card dataset: Python (Pandas, advanced modules e.g., Matplotlib), MariaDB, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries. Learners will be expected to set up their environments and perform installations on their local machines. 

## Credit Card Dataset Overview
The Credit Card System database is an independent system developed for managing activities such as registering new customers and approving or canceling requests, etc., using the architecture.
A credit card is issued to users to enact the payment system. It allows the cardholder to access financial services in exchange for the holder's promise to pay for them later. Below are three files that contain the customer’s transaction information and inventories in the credit card information.
CDW_SAPP_CUSTOMER.JSON: This file has the existing customer details.
CDW_SAPP_CREDITCARD1.JSON: This file contains all credit card transaction information.
CDW_SAPP_BRANCH.JSON: Each branch’s information and details are recorded in this file. 
```
I have used Jupyter Notebook for this project.
```

### Business Requirements - ETL
For “Credit Card System,” create a Python and PySpark SQL program to read/extract the following JSON files according to the specifications found in the mapping document.
And then all files loaded into database.

```
Convert DAY, MONTH, and YEAR into a TIMEID (YYYYMMDD) - Used Regex to convert to TIMEID.
Also format of phonenumber chaged using regex.
```

### Transaction Details Module

```
1)Used to display the transactions made by customers living in a given zip code for a given month and year. 
 - Imported mysql.connecter and wrote function to connect with database. 
 - Wrote sql query for the given condition to fetch the data from database by giving input for zipcode, year and month.
```
