# Capstone-Project

# Project Summary

The Project is to build an Datawarehouse where data is taken from three different sources, build ETL pipleline for the I94 Immigration, U.S. City Demographic and World Temperature data datasets using Pyspark. We create the star schema with the four dimension tables and one fact table and the results being saved in paraquet files for the downstream analysis which is helpful to the analytics team.

## Dimension Tables:
1. Cities
2. Immigrants
3. Monthly average city temperature
4. Time

## Fact Table:
1. Immigration

Data is taken from three different sources

## Source Data:
I94 Immigration Data: Comes from the U.S. National Tourism and Trade Office and contains various statistics on international visitor arrival in USA and comes from the US National Tourism and Trade Office. The dataset contains data from 2016.
World Temperature Data: Comes from Kaggle and contains average weather temperatures by city.
U.S. City Demographic Data: Comes from OpenSoft and contains information about the demographics of all US cities such as average age, male and female population.

## DATA MODELLING
Created the star schema with the four dimension tables and one fact tables for quering the data.

First we create the staging tables as below

## Staging Tables
staging_immi_i94_df
    id
    date
    state_code
    city_code
    gender
    age 
    visa_type
    count
    
staging_temperature_df
    year
    month
    city_name
    city_code
    avg_temperature
    lat
    long
    
staging_demog_df
    city_code
    state_code
    city_name
    median_age
    pct_male_pop
    pct_female_pop
    pct_veterans
    pct_foreign_born
    pct_native_american
    pct_asian
    pct_black
    pct_hispanic_or_latino
    pct_white
    total_pop

### Dimension Tables
dim_immigrant_df
    id
    gender
    age
    visa_type
    
dim_city_df
    city_code
    state_code
    city_name
    median_age
    pct_male_pop
    pct_female_pop
    pct_veterans
    pct_foreign_born
    pct_native_american
    pct_asian
    pct_black
    pct_hispanic_or_latino
    pct_white
    total_pop
    lat
    long
    
dim_monthly_city_temp_df
    city_code
    year
    month
    avg_temperature
    
dim_time_df
    date
    dayofweek
    weekofyear
    month

## Fact Table
fact_immigration_df
    id
    state_code
    city_code
    date
    count

## ETL PIPELINE
1. The data need to be cleaned which has nulls,duplicates
2. The data need to be loaded into the staging tables from the sources into staging_i94_df, staging_temp_df and staging_demo_df
3. We need to create the dimension tables dim_immigrant_df,dim_monthly_city_temp_df, dim_city_df and dim_time_df and load the data
4. We need to create the fact tabe fact_immigration_df and load the data.
5. The proceesed data from dimension and fact tables are created the parquet files for the downstream query for analytics



 











































