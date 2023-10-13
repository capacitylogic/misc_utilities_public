"""
Copyright 2023 Capacity Logic, LLC

Licensed under the MIT license;
you may not use this file except in compliance with the License.

This software is distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, 
WITHOUT ANY POSSIBLE EXCEPTIONS. If you believe you are subject to a legal jurisdiction 
that requires an exception to this limitation then you are not eligible for this license and 
may not use or distribute this software in any way.
"""

"""
This is a utility that queries a given table in a Google BigQuery data base containing one or more
JSON columns, flattens those JSONs such that each key is a column name, and returns a final SQL statement
selecting all columns from the original table, plus a column for each key of each JSON column.

This was originally written to speed up creation of dbt models where the source has JSON columns.  
For example, many of the properties (i.e., fields) in HubSpot tables are loaded into data warehouses 
as JSON columns by extract/load tools.

You will need appropriate access to a BigQuery instance in Google Cloud.  If you have set up the Google Cloud 
CLI, you can run `gcloud auth application-default login` (*nix) to authenticate via a browser. Or you can modify 
this code to utilize a service account.
"""



import json
from google.cloud import bigquery


def construct_flattened_sql(project_id, dataset_id, table_id):
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Get all JSON columns from the table
    json_columns_query = f"""
        SELECT column_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_id}' AND data_type = 'JSON';
    """
    
    query_job = client.query(json_columns_query)
    result = query_job.result()
    json_cols = [row.column_name for row in result]

    # List to store the newly constructed columns
    new_columns = []

    # Retrieve one non-null row for each JSON column and extract its keys
    for col in json_cols:
        query_string = f"""
            SELECT {col}
            FROM `{project_id}.{dataset_id}.{table_id}`
            WHERE {col} IS NOT NULL
            LIMIT 1;
        """

        query_job = client.query(query_string)
        result = query_job.result()
        rows = list(result)

        # Check if the result has any rows
        if not rows:
            continue

        row = rows[0]
        json_string = getattr(row, col)
        
        json_data = json.loads(json_string)
        keys = list(json_data.keys())
        new_columns.extend(keys)

    # SQL to get all column names in the table
    all_columns_query = f"""
        SELECT column_name 
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_id}';
    """

    query_job = client.query(all_columns_query)
    result = query_job.result()
    all_columns_in_table = [row.column_name for row in result]

    # Combine all columns and sort alphabetically
    combined_columns = sorted(all_columns_in_table + new_columns)

    # Adjust duplicate column names by appending integers
    seen = {}
    final_columns = []
    for col in combined_columns:
        if combined_columns.count(col) > 1 and col in seen:
            seen[col] += 1
            final_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 1
            final_columns.append(col)

    # Construct the final SQL
    final_sql = f"SELECT {', '.join(final_columns)} FROM `{project_id}.{dataset_id}.{table_id}`;"
    return final_sql


# Example usage:
project_id = 'PROJECT_ID'
dataset_id = 'DATASET_ID'
table_id = 'TABLE_ID'
sql_string = construct_flattened_sql(project_id, dataset_id, table_id)
print(sql_string)

# Save sql_string to a .sql file
file_name = f"{table_id}_flattened.sql"
with open(file_name, 'w') as file:
    file.write(sql_string)

print(f"SQL saved to {file_name}")
