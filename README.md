# Kafka + Spark Streaming Analytics Project

## Overview

This project combines the use of **Kafka**, **Spark Structured Streaming**, and **PostgreSQL** to build a real-time analytics pipeline for website user behavior data.

The system uses **Spark** to consume streaming data from **Kafka**, process and aggregate the data, and store the analytical results into a **PostgreSQL** database for reporting purposes.

---

# Problem Statement

## Input

### Kafka
- A local Kafka cluster
- A Kafka topic containing website user behavior events generated from the Kafka module project

### Spark
- A local Spark cluster used for streaming data processing

### Input Data Schema
The incoming Kafka messages follow the schema below.

---

# Input Data Schema

| Column Name   | Data Type      | Description | Example |
|----------------|----------------|-------------|----------|
| `id` | String | Log ID | `aea4b823-c5c6-485e-8b3b-6182a7c4ecce` |
| `api_version` | String | API version | `1.0` |
| `collection` | String | Log type | `view_product_detail` |
| `current_url` | String | Current webpage URL visited by the user | `https://www.glamira.cl/glamira-anillo-saphira-skug100335.html?...` |
| `device_id` | String | Device ID | `874db849-68a6-4e99-bcac-fb6334d0ec80` |
| `email` | String | User email address | |
| `ip` | String | User IP address | `190.163.166.122` |
| `local_time` | String | Event creation time (`yyyy-MM-dd HH:mm:ss`) | `2024-05-28 08:31:22` |
| `option` | Array<Object> | Product option list | `[{"option_id":"328026","option_label":"diamond"}]` |
| `product_id` | String | Product ID | `96672` |
| `referrer_url` | String | Referrer URL leading to the current page | `https://www.google.com/` |
| `store_id` | String | Store ID | `85` |
| `time_stamp` | Long | Event timestamp | |
| `user_agent` | String | Browser and device information | `Mozilla/5.0 (iPhone; CPU iPhone OS 13_4_1 like Mac OS X)...` |

---

# Expected Output

The project should provide:

- Database design for storing analytical results
- Spark streaming application code
- Real-time analytical reports
- Processed data stored in PostgreSQL

---

# Reporting Requirements

The system must generate the following reports:

## 1. Top 10 Most Viewed Products (Current Day)

Retrieve the top 10 `product_id` values with the highest number of views during the current day.

---

## 2. Top 10 Countries by Views (Current Day)

Retrieve the top 10 countries with the highest number of views during the current day.

> Country information should be derived from the website domain.

---

## 3. Top 5 Referrer URLs

Retrieve the top 5 `referrer_url` values with the highest number of views during the current day.

---

## 4. Store Views by Country

For a given country:
- Retrieve the list of `store_id`
- Calculate the corresponding number of views
- Sort results by descending view count

---

## 5. Hourly View Distribution for a Product

For a given `product_id`:
- Calculate the number of views grouped by hour during the day

---

## 6. Hourly Views by Browser and Operating System

Generate hourly aggregated view statistics grouped by:
- Browser
- Operating System (`os`)

---

# Suggested Technology Stack

- **Apache Kafka**
- **Apache Spark Structured Streaming**
- **PostgreSQL**
- **Docker**
- **Python / PySpark**

---

# Suggested Architecture

```text
Kafka Topic
      ↓
Spark Structured Streaming
      ↓
Data Transformation & Aggregation
      ↓
PostgreSQL
      ↓
Analytics & Reporting
