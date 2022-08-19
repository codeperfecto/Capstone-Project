
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.types import *

def drop_empty_columns(df, columns=[]):
    """
     Drop null,none columns
        :param df: It is dataframe
        :param columns: List of columns that need to be checked
        :return: output updated dataframe
    """
    print("The missing data is dropped...")
    df.na.drop(how="any", subset=columns).show(truncate=False)
    
    return df

def drop_duplicate_rows(df, cols=[]):
    """
    Drop duplicate columns
    :param df: dataframe
    :param columns: columns list to check
    :return: updated dataframe
    
    """
    df = df.drop_duplicates()
    return df

   
def data_quality_check(df, table):
    """
     Data Quality check perfromed
        :param df: It is a spark dataframe to check  record counts
        :param table: name of table
    """    
    record_count = df.count()

    if (record_count!= 0):
        print("Data quality check passed for {} with record_count: {} records.".format(table, record_count))    
    else:
        print("Data quality check failed for {} with zero records!".format(table))
        
    return 0
