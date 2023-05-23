# snowflake-streaming-api-python

### Introduction: 

In today's data-driven world, real-time analytics and insights have become crucial for businesses. To
facilitate this, organizations often need to stream data from various sources into a central data
warehouse. However, building infrastructure and code for streaming pipelines can get very challenging
because of the complex nature of streaming architectures which often involves integrating with tools
such as Apache Kafka, Flink, Spark etc. as well building the expertise to write high quality code to
integrate and manage these technologies.

In this article, we will explore the process of streaming data from MySQL to Snowflake Data Cloud
platform using the Snowflake’s Streaming Ingest API SDK. This new Streaming Ingest SDK from
Snowflake is built on Java and so it requires the user to have expertise in Java programming in order to
build an app that can stream data directly into Snowflake. However, this guide will provide you with a
step-by-step approach to achieve seamless and efficient data streaming just using python and pandas
dataframes.


<img width="1267" alt="Screenshot 2023-05-22 at 7 22 24 PM" src="https://github.com/vishalp9758/snowflake-streaming-api-python/assets/121073802/b6f01ef9-8bbc-4862-aae9-fbb600b54d33">

#### Prerequisites:
Before diving into the streaming process, ensure that you have the following prerequisites in place:
1. A working MySQL database with the data you want to stream.
2. A Snowflake account with the necessary privileges to create and manage objects.
3. Anaconda Python version 3.8. 

#### Step 1: Set up the Snowflake Account and Database:
To begin, make sure you have a Snowflake account. If you don't have one, sign up for a free trial or set up a production account. Once you have access to Snowflake, create a database and a schema to which you'll stream the MySQL data. 

<img width="252" alt="image" src="https://github.com/vishalp9758/snowflake-streaming-api-python/assets/121073802/c692536b-f4c9-4685-9b02-45615f88f9a8">

In my workflow I had uploaded a sample data set into a MySQL database hosted on Azure. Following is the schema and some sample records from my source table in MySQL database.
 
<img width="225" alt="image" src="https://github.com/vishalp9758/snowflake-streaming-api-python/assets/121073802/5c48c43d-062d-419a-9feb-1724af1ac9e0">

<img width="468" alt="image" src="https://github.com/vishalp9758/snowflake-streaming-api-python/assets/121073802/41aaaf19-16e5-4815-91ba-d75a4b0504dd">


#### Step 2: Create a Conda environment for Python 3.8:
Create an anaconda python environment for python v3.8 and install the following libraries using the snowflake anaconda channel – 

```
conda create --name snowpark_java -c https://repo.anaconda.com/pkgs/snowflake python=3.8
conda activate snowpark_java
```

Next install Snowpark for python, Pyjnius, mysql-connector-python & phonenumbers

```
conda install -c https://repo.anaconda.com/pkgs/snowflake snowflake-snowpark-python pandas
conda install  pyjnius, mysql-connector-python, phonenumbers
```

#### Step 3: Update the “snowflake_env_login_creds.json” file with your snowflake platform credentials and source database credentials:
You will need to generate private & public keys and register them in your snowflake account to security connect using the API. Please use the following link to generate and configure key pair authentication.
https://docs.snowflake.com/en/user-guide/key-pair-auth#configuring-key-pair-authentication

#### Step 4: Configure path to JAR files required to use the Streaming API SDK:

<img width="468" alt="image" src="https://github.com/vishalp9758/snowflake-streaming-api-python/assets/121073802/7bdbe4ea-76d3-43dc-9e39-b8fbc0908f23">
 

#### Step 5: Set up MySQL Database Connection:
Next, establish a connection to your MySQL database. You can use any Python library that supports MySQL, such as `mysql-connector-python`, to connect to MySQL. Ensure that you have the necessary credentials (host, port, username, password, database name) to connect to the MySQL database.

<img width="468" alt="image" src="https://github.com/vishalp9758/snowflake-streaming-api-python/assets/121073802/8d0772dc-4283-409e-acc7-61ea4463fb59">
 

#### Step 6: Set up the Snowflake Streaming Ingest API Connection:
In order to use the Snowflake Streaming Ingest API, you first need to authenticate and establish a secure connection to Snowflake. Follow these steps below to set up a connection and to create a secure client to stream data directly into a Snowflake table:
 
 <img width="468" alt="image" src="https://github.com/vishalp9758/snowflake-streaming-api-python/assets/121073802/74d9d246-07af-4839-8ea5-8ca7fb3ae490">

#### Step 7: Prepare a payload for streaming:
The payload that is streamed into snowflake needs to be packaged as a Java hashmap, so create a hashmap that contains values for all the columns in the destination table as shown below – 
 
<img width="468" alt="image" src="https://github.com/vishalp9758/snowflake-streaming-api-python/assets/121073802/9b21b7a0-2296-4019-8b56-4d5aea60518f">
  
Snowflake’s Streaming API supports both single row inserts and bulk inserts

#### Step 8: Use Snowpark to perform data quality checks:
Connect to your Snowflake account using Snowpark and validate that all the records from the source MySQL table were successfully ingested into the destination table in Snowflake.

<img width="468" alt="image" src="https://github.com/vishalp9758/snowflake-streaming-api-python/assets/121073802/5869c72f-bdc5-4cfe-bf79-3fa7b667e54e">
