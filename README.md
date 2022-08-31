# CapstoneProject

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
 For “Credit Card System,” create a Python and PySpark SQL program to read/extract the following JSON files according to the specifications found in the mapping     document.And then all files loaded into database.

  - Convert DAY, MONTH, and YEAR into a TIMEID (YYYYMMDD) - Used Regex to convert to TIMEID.
  - Also phonenumber format changed using regex.

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
```
https://user-images.githubusercontent.com/104329174/187571317-900498dc-d7fa-4d89-898b-45d17e57b637.png
```
2) Find and plot states, showing which state has the highest number of customers.
```
https://user-images.githubusercontent.com/104329174/187571499-167c15f9-254d-4641-8136-5e05b1719ddb.png
```
3) Find and plot the sum of all transactions for each customer, and which customer has the highest transaction amount. (First 20)
   hint(use CUST_SSN).
   ```
   https://user-images.githubusercontent.com/104329174/187571638-9dd58a5d-ba70-4c4b-8df3-b33bf2e466b4.png
   ```
4) Find and plot the top three months with the largest transaction data.
```
https://user-images.githubusercontent.com/104329174/187571742-8772598d-d041-4c73-9b3a-8f88f26791b7.png
```
5) Find and plot each branches healthcare transactions, showing which branch  processed the highest total dollar value of healthcare transactions.
```
https://user-images.githubusercontent.com/104329174/187571842-1788268e-48ac-4736-b839-cd4a5fcc8ea1.png
```
### Tableau Dashboard
- Created Tableau Dashboard for the above requirements.
```
https://user-images.githubusercontent.com/104329174/187574323-0340d2c9-5be2-402c-a3b9-c7a132efb37c.png

https://user-images.githubusercontent.com/104329174/187574549-5dfc3121-23f4-4499-8c34-52b381735c0b.png
(Ex. Based on selecting the state from CUSTOMERS PER STATE, the other graphs will also change based on the selection)
```

## 4. Functional Requirements - LOAN Application Dataset
1) Create a Python program to GET (consume) data from the above API endpoint for the loan application dataset.
- Used requests.get(url) to get API data.
2) Find the status code of the above API endpoint.
 - Used .status_code to show the status.
3) Once Python reads data from the API, utilize PySpark to load data into RDBMS(SQL). The table name should be CDW-SAPP_loan_application in the database.
  - Data loaded into database after importing mysql.connecter.
### 5. Functional Requirements - Data Analysis and Visualization for Loan Application
1) Create a bar chart that shows the difference in application approvals for Married Men vs Married Women based on income ranges. (number of approvals)
```
https://user-images.githubusercontent.com/104329174/187573271-2a4897bb-3eff-497c-aa0d-b08015c7c14b.png
```
2) Create and plot a chart that shows the difference in application approvals based on Property Area.
```
https://user-images.githubusercontent.com/104329174/187573373-8f9fbc3e-2f45-4de6-8fd5-1549ec90918d.png
```
3) Create a multi-bar plot that shows the total number of approved applications per each application demographic. 
```
https://user-images.githubusercontent.com/104329174/187573532-1011d93c-1037-401f-b77a-74c1b105960a.png

```
###Tableau Dashboard for Loan Data
- Created Dashboard in Tableau for Approval Status 
```
https://user-images.githubusercontent.com/104329174/187579478-21e8b787-8cb0-4311-b282-e14c89fc46e5.png

https://user-images.githubusercontent.com/104329174/187579607-4386994f-1aa5-458e-be46-af3998a8a639.png
(Ex. Based on the selection of Application status, it will change the other graphs based on the selection) 
