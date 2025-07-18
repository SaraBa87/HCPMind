# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This file contains the tools used by the database agent."""

import datetime
import logging
import os
import re
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

from google.adk.tools import ToolContext
from google.cloud import bigquery
from google.genai import Client


# Assume that `BQ_PROJECT_ID` is set in the environment. See the
# `data_agent` README for more details.
project = os.getenv("BQ_PROJECT_ID", None)
location = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")

# Get API key from environment variable
google_api_key = os.getenv("GOOGLE_API_KEY")
if not google_api_key:
    raise ValueError("GOOGLE_API_KEY environment variable is not set")

# Initialize the Google AI client with the API key
llm_client = Client(api_key=google_api_key)

MAX_NUM_ROWS = 80


database_settings = None
bq_client = None
def get_env_var(var_name):
  """Retrieves the value of an environment variable.

  Args:
    var_name: The name of the environment variable.

  Returns:
    The value of the environment variable, or None if it is not set.

  Raises:
    ValueError: If the environment variable is not set.
  """
  try:
    value = os.environ[var_name]
    return value
  except KeyError:
    raise ValueError(f'Missing environment variable: {var_name}')

def get_bq_client():
    """Get BigQuery client."""
    global bq_client
    if bq_client is None:
        bq_client = bigquery.Client()
    return bq_client


def get_database_settings():
    """Get database settings."""
    global database_settings
    if database_settings is None:
        database_settings = update_database_settings()
    return database_settings


def update_database_settings():
    """Update database settings."""
    global database_settings
    ddl_schema = get_bigquery_schema()
    database_settings = {
        "bq_ddl_schema": ddl_schema,
    }
    return database_settings


def get_bigquery_schema():
    """Retrieves schema and generates DDL for the public dataset.

    Returns:
        str: A string containing the generated DDL statements.
    """
    client = bigquery.Client()
    dataset_id = "bigquery-public-data.cms_synthetic_patient_data_omop"
    
    ddl_statements = ""
    
    # List all tables in the dataset
    tables = client.list_tables(dataset_id)
    
    for table in tables:
        table_ref = f"{dataset_id}.{table.table_id}"
        table_obj = client.get_table(table_ref)

        # Check if table is a view
        if table_obj.table_type != "TABLE":
            continue

        ddl_statement = f"CREATE OR REPLACE TABLE `{table_ref}` (\n"

        for field in table_obj.schema:
            ddl_statement += f"  `{field.name}` {field.field_type}"
            if field.mode == "REPEATED":
                ddl_statement += " ARRAY"
            if field.description:
                ddl_statement += f" COMMENT '{field.description}'"
            ddl_statement += ",\n"

        ddl_statement = ddl_statement[:-2] + "\n);\n\n"

        # Add example values if available (limited to first row)
        rows = client.list_rows(table_ref, max_results=5).to_dataframe()
        if not rows.empty:
            ddl_statement += f"-- Example values for table `{table_ref}`:\n"
            for _, row in rows.iterrows():
                ddl_statement += f"INSERT INTO `{table_ref}` VALUES\n"
                example_row_str = "("
                for value in row.values:
                    if isinstance(value, str):
                        example_row_str += f"'{value}',"
                    elif value is None:
                        example_row_str += "NULL,"
                    else:
                        example_row_str += f"{value},"
                example_row_str = example_row_str[:-1] + ");\n\n"  # remove trailing comma
                ddl_statement += example_row_str

        ddl_statements += ddl_statement

    return ddl_statements


def initial_bq_nl2sql(question: str) -> Dict[str, Any]:
    """Generates an initial SQL query from a natural language question.

    Args:
        question (str): Natural language question.

    Returns:
        Dict[str, Any]: Dictionary containing:
            - explain: Explanation of how the SQL was generated
            - sql: Generated SQL query
            - sql_results: Results from executing the SQL (if available)
            - nl_results: Natural language explanation of results (if available)
    """
    prompt_template = """
You are a BigQuery SQL expert tasked with answering user's questions about BigQuery tables by generating SQL queries in the GoogleSql dialect. Your task is to write a Bigquery SQL query that answers the following question while using the provided context.

**Guidelines:**
- **Table Referencing:** Always use the full table name with the dataset prefix in the SQL statement. Tables should be referred to using a fully qualified name enclosed in backticks (`) e.g. `bigquery-public-data.cms_synthetic_patient_data_omop.table_name`. Table names are case sensitive.
- **Joins:** Join as few tables as possible. When joining tables, ensure all join columns are the same data type. Analyze the database and the table schema provided to understand the relationships between columns and tables.
- **Aggregations:** Use all non-aggregated columns from the `SELECT` statement in the `GROUP BY` clause.
- **SQL Syntax:** Return syntactically and semantically correct SQL for BigQuery. Use SQL `AS` statement to assign a new name temporarily to a table column or even a table wherever needed. Always enclose subqueries and union queries in parentheses.
- **Column Usage:** Use *ONLY* the column names (column_name) mentioned in the Table Schema. Do *NOT* use any other column names. Associate `column_name` mentioned in the Table Schema only to the `table_name` specified under Table Schema.
- **FILTERS:** Write queries effectively to reduce and minimize the total rows to be returned. Use filters (like `WHERE`, `HAVING`, etc.) and aggregations (like 'COUNT', 'SUM', etc.) appropriately.
- **LIMIT ROWS:** The maximum number of rows returned should be less than 1000.

**Schema:**
The database structure is defined by the following table schemas:

```
{SCHEMA}
```

**Natural language question:**
```
{QUESTION}
```

**Think Step-by-Step:** Carefully consider the schema, question, guidelines, and best practices outlined above to generate the correct BigQuery SQL.
"""

    # Get database settings to provide context
    db_settings = get_database_settings()
    ddl_schema = db_settings["bq_ddl_schema"]

    # Format the prompt with the schema and question
    prompt = prompt_template.format(
        SCHEMA=ddl_schema,
        QUESTION=question
    )

    # Generate SQL using the LLM
    response = llm_client.models.generate_content(
        model=os.getenv("BIGQUERY_AGENT_MODEL"),
        contents=prompt,
        config={"temperature": 0.1},
    )

    # Extract and clean the SQL
    sql = response.text
    if sql:
        sql = sql.replace("```sql", "").replace("```", "").strip()

    # Return the results in the expected format
    return {
        "explain": f"Generated SQL for question: {question}",
        "sql": sql,
        "sql_results": None,  # Will be populated by run_bigquery_validation
        "nl_results": None    # Will be populated by run_bigquery_validation
    }


def run_bigquery_validation(
    sql_string: str,
    tool_context: ToolContext,
) -> str:
    """Validates BigQuery SQL syntax and functionality.

    This function validates the provided SQL string by attempting to execute it
    against BigQuery in dry-run mode. It performs the following checks:

    1. **SQL Cleanup:**  Preprocesses the SQL string using a `cleanup_sql`
    function
    2. **DML/DDL Restriction:**  Rejects any SQL queries containing DML or DDL
       statements (e.g., UPDATE, DELETE, INSERT, CREATE, ALTER) to ensure
       read-only operations.
    3. **Syntax and Execution:** Sends the cleaned SQL to BigQuery for validation.
       If the query is syntactically correct and executable, it retrieves the
       results.
    4. **Result Analysis:**  Checks if the query produced any results. If so, it
       formats the first few rows of the result set for inspection.

    Args:
        sql_string (str): The SQL query string to validate.
        tool_context (ToolContext): The tool context to use for validation.

    Returns:
        str: A message indicating the validation outcome. This includes:
             - "Valid SQL. Results: ..." if the query is valid and returns data.
             - "Valid SQL. Query executed successfully (no results)." if the query
                is valid but returns no data.
             - "Invalid SQL: ..." if the query is invalid, along with the error
                message from BigQuery.
    """

    def cleanup_sql(sql_string):
        """Processes the SQL string to get a printable, valid SQL string."""

        # 1. Remove backslashes escaping double quotes
        sql_string = sql_string.replace('\\"', '"')

        # 2. Remove backslashes before newlines (the key fix for this issue)
        sql_string = sql_string.replace("\\\n", "\n")  # Corrected regex

        # 3. Replace escaped single quotes
        sql_string = sql_string.replace("\\'", "'")

        # 4. Replace escaped newlines (those not preceded by a backslash)
        sql_string = sql_string.replace("\\n", "\n")

        # 5. Add limit clause if not present
        if "limit" not in sql_string.lower():
            sql_string = sql_string + " limit " + str(MAX_NUM_ROWS)

        return sql_string

    logging.info("Validating SQL: %s", sql_string)
    sql_string = cleanup_sql(sql_string)
    logging.info("Validating SQL (after cleanup): %s", sql_string)

    final_result = {"query_result": None, "error_message": None}

    # More restrictive check for BigQuery - disallow DML and DDL
    if re.search(
        r"(?i)(update|delete|drop|insert|create|alter|truncate|merge)", sql_string
    ):
        final_result["error_message"] = (
            "Invalid SQL: Contains disallowed DML/DDL operations."
        )
        return final_result

    try:
        query_job = get_bq_client().query(sql_string)
        results = query_job.result()  # Get the query results

        if results.schema:  # Check if query returned data
            rows = [
                {
                    key: (
                        value
                        if not isinstance(value, datetime.date)
                        else value.strftime("%Y-%m-%d")
                    )
                    for (key, value) in row.items()
                }
                for row in results
            ][
                :MAX_NUM_ROWS
            ]  # Convert BigQuery RowIterator to list of dicts
            # return f"Valid SQL. Results: {rows}"
            final_result["query_result"] = rows

            tool_context.state["query_result"] = rows

        else:
            final_result["error_message"] = (
                "Valid SQL. Query executed successfully (no results)."
            )

    except (
        Exception
    ) as e:  # Catch generic exceptions from BigQuery  # pylint: disable=broad-exception-caught
        final_result["error_message"] = f"Invalid SQL: {e}"

    print("\n run_bigquery_validation final_result: \n", final_result)

    return final_result

