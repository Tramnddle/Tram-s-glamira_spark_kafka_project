Project Requirements
Overview

The problem combines the use of Kafka and Spark. Use Spark to read data from Kafka, then process, calculate, and store the results into a Postgres database.

Problem Statement

Input:

Kafka: A Kafka cluster set up locally, with a topic containing website user behavior data from the Kafka module project
Spark: A Spark cluster installed locally
Data schema

Output:

Database design
Program code to process project requirements
Report results according to the requirements
Data stored in the Postgres database
Description
Input Data Schema
Name	Data Type	Description	Example
id	String	Log id	aea4b823-c5c6-485e-8b3b-6182a7c4ecce
api_version	String	API version	1.0
collection	String	Log type	view_product_detail
current_url	String	URL of the webpage the user is visiting	https://www.glamira.cl/glamira-anillo-saphira-skug100335.html?alloy=white-375&diamond=sapphire&stone2=diamond-Brillant&itm_source=recommendation&itm_medium=sorting
device_id	String	Device id	874db849-68a6-4e99-bcac-fb6334d0ec80
email	String	User email	
ip	String	IP address	190.163.166.122
local_time	String	Time the log was created. Format: yyyy-MM-dd HH:mm:ss	2024-05-28 08:31:22
option	Array<Object>	List of product options	[{"option_id": "328026", "option_label": "diamond"}]
product_id	String	Product id	96672
referrer_url	String	Webpage leading to current_url	https://www.google.com/
store_id	String	Store id	85
time_stamp	Long	Timestamp when the log record was created	
user_agent	String	Browser and device information	Mozilla/5.0 (iPhone; CPU iPhone OS 13_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1

Requirements

Design the database and write a program to generate the following reports:

Top 10 product_id with the highest number of views on the current day
Top 10 countries with the highest number of views on the current day (country determined based on domain)
Top 5 referrer_url with the highest number of views on the current day
For any given country, retrieve the list of store_id and corresponding view counts, sorted by descending view count
Hourly view distribution for any given product_id during the day
Hourly view data by browser and os
