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
For “Credit Card System,” create a Python and PySpark SQL program to read/extract the following JSON files according to the specifications found in the mapping document.And then all files loaded into database.

--Convert DAY, MONTH, and YEAR into a TIMEID (YYYYMMDD) - Used Regex to convert to TIMEID.
--Also phonenumber format changed using regex.

### 2.1 Transaction Details Module

1) Used to display the transactions made by customers living in a given zip code for a given month and year. 
 - Imported mysql.connecter and wrote function to connect with database. 
 - Wrote sql query for the given condition to fetch the data from database by giving input for zipcode, year and month.
 2) Used to display the number and total values of transactions for a given type.
 - Wrote sql query for the given condition to fetch the data from database by giving input for transaction type.
3) Used to display the number and total values of transactions for branches in a given state.
 - Wrote sql query for the given condition to fetch the data from database by giving input for state.

### 2.2 Customer Details Module

1) Used to check the existing account details of a customer.
2) Used to modify the existing account details of a customer.
3) Used to generate a monthly bill for a credit card number for a given month and year.
4) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
-  Wrote sql query for the given condition to fetch the data from database by giving input details.

### 3. Functional Requirements - Data analysis and Visualization

1) Find and plot transactions, showing which transaction type occurs most often.
2) Find and plot states, showing which state has the highest number of customers.
3) Find and plot the sum of all transactions for each customer, and which customer has the highest transaction amount. (First 20)
   hint(use CUST_SSN).
4) Find and plot the top three months with the largest transaction data.
5) Find and plot each branches healthcare transactions, showing which branch  processed the highest total dollar value of healthcare transactions.

## 4. Functional Requirements - LOAN Application Dataset
1) Create a Python program to GET (consume) data from the above API endpoint for the loan application dataset.
2) Find the status code of the above API endpoint.
3) Once Python reads data from the API, utilize PySpark to load data into RDBMS(SQL). The table name should be CDW-SAPP_loan_application in the database.

### 5. Functional Requirements - Data Analysis and Visualization for Loan Application
1) Create a bar chart that shows the difference in application approvals for Married Men vs Married Women based on income ranges. (number of approvals)
2) Create and plot a chart that shows the difference in application approvals based on Property Area.
3) Create a multi-bar plot that shows the total number of approved applications per each application demographic. 
