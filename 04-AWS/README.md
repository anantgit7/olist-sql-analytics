**AWS – MySQL – Power BI Integration**

**This folder contains the setup and instructions for connecting AWS RDS (MySQL) to Power BI.
The goal is to use live cloud data for analysis and dashboards.**

Components:

**1.	AWS RDS Connection**

⦁	A MySQL database is hosted on AWS RDS.

⦁	Public access enabled and IP whitelisted.

⦁	Used for storing and querying transactional data.

**2) MySQL Client Connection**

⦁	MySQL Workbench is used to run SQL queries, inspect tables, and verify results.

⦁	Same database credentials are used for Power BI.

**3) ODBC Connector for Power BI**

⦁	MySQL ODBC driver installed to allow Power BI to communicate with RDS.

⦁	A DSN is created using the RDS endpoint, username, and password.

⦁	Power BI “Get Data → ODBC” is used to load data into reports.

This setup allows secure and real-time access to cloud data for reporting and visualization in Power BI.
