#!/usr/bin/env python
# coding: utf-8

# In[6]:


from sqlalchemy import create_engine
import pandas as pd

# Replace these with your actual connection details
oracle_username = 'SYSTEM'
oracle_password = '1234'
oracle_host = 'DESKTOP-CCM4VHF'
oracle_port = 1521  # Oracle's default port is 1521
oracle_service_name = 'XE'

# Oracle connection
oracle_connection_string = f'oracle+cx_oracle://{oracle_username}:{oracle_password}@{oracle_host}:{oracle_port}/{oracle_service_name}'
oracle_engine = create_engine(oracle_connection_string)

# PostgreSQL connection details
postgresql_username = 'postgres'
postgresql_password = '1234'
postgresql_host = 'localhost'
postgresql_port = 5432  # PostgreSQL's default port is 5432
postgresql_database = 'weather'

# PostgreSQL connection
postgresql_connection_string = f'postgresql://{postgresql_username}:{postgresql_password}@{postgresql_host}:{postgresql_port}/{postgresql_database}'
postgresql_engine = create_engine(postgresql_connection_string)

# Query data from Oracle
query = "SELECT * FROM SYSTEM.weather_details"
df = pd.read_sql(query, oracle_engine)

# Write data to PostgreSQL
df.to_sql('weather_details', postgresql_engine, if_exists='replace', index=False)

