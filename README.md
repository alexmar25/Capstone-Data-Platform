Capstone Data Platform Project
Overview
This project simulates the data platform architecture of an e-commerce company, SoftCart, that operates with a hybrid architecture involving both on-premises and cloud-based solutions. It integrates multiple technologies to handle online transactions, data warehousing, big data analytics, and business intelligence reporting.

The goal of this project is to showcase end-to-end data engineering and analytics skills, including database management, ETL pipelines, big data processing, and interactive dashboards.

Technologies Used
<table> <tr> <th>Category</th> <th>Tools</th> </tr> <tr> <td>Databases</td> <td>MySQL, MongoDB, PostgreSQL, IBM DB2 (Cloud)</td> </tr> <tr> <td>Big Data</td> <td>Hadoop, Apache Spark</td> </tr> <tr> <td>ETL & Data Pipelines</td> <td>Apache Airflow</td> </tr> <tr> <td>Business Intelligence</td> <td>IBM Cognos Analytics</td> </tr> </table>
Project Architecture
<details> <summary>Click to expand</summary>
Online Presence:

SoftCart's e-commerce website serves customers across various devices (laptops, mobiles, tablets).
The website is powered by two databases:
Product Catalog Data: Stored in a MongoDB NoSQL server.
Transactional Data (Inventory & Sales): Stored in a MySQL database.
Data Flow:

ETL Pipelines: Data is periodically extracted from the OLTP (MySQL) and NoSQL (MongoDB) databases and staged in a PostgreSQL data warehouse.
Production Warehouse: Data is processed and moved to an IBM DB2 cloud instance for final analytics and reporting.
Big Data Integration:

A Hadoop cluster collects all data for long-term storage and big data processing.
Spark performs analytics on the Hadoop cluster for deeper insights.
Business Intelligence:

BI dashboards are built using IBM Cognos Analytics, connected directly to the IBM DB2 production data warehouse.
</details>
