from google.cloud import bigquery
import os
from dotenv import load_dotenv
from pathlib import Path
import sys

# Add the parent directory to the Python path
sys.path.append(str(Path(__file__).parent.parent))

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

def test_bigquery_connection():
    """Test basic BigQuery connection and query execution."""
    try:
        client = bigquery.Client()
        query = """
        SELECT *
        FROM `bigquery-public-data.cms_synthetic_patient_data_omop.cost`
        LIMIT 5
        """
        query_job = client.query(query)
        results = query_job.to_dataframe()
        print("\nTest 1: Basic BigQuery Connection")
        print("Successfully connected to BigQuery and executed query")
        print(f"Number of rows returned: {len(results)}")
        print("First row:", results.iloc[0].to_dict())
        return True
    except Exception as e:
        print(f"Error in test_bigquery_connection: {str(e)}")
        return False

def test_get_database_settings():
    """Test getting database settings."""
    try:
        from mutil_tool_agent.tools import get_database_settings
        settings = get_database_settings()
        print("\nTest 2: Database Settings")
        print("Successfully retrieved database settings")
        print("Settings:", settings)
        return True
    except Exception as e:
        print(f"Error in test_get_database_settings: {str(e)}")
        return False

def test_initial_bq_nl2sql():
    """Test SQL generation from natural language."""
    try:
        from mutil_tool_agent.tools import initial_bq_nl2sql
        question = "show me top 5 rows of cost table"
        result = initial_bq_nl2sql(question)
        print("\nTest 3: SQL Generation")
        print("Successfully generated SQL from question")
        print("Question:", question)
        print("Generated SQL:", result["sql"])
        print("Explanation:", result["explain"])
        return True
    except Exception as e:
        print(f"Error in test_initial_bq_nl2sql: {str(e)}")
        return False

def test_run_bigquery_validation():
    """Test SQL validation and execution."""
    try:
        from mutil_tool_agent.tools import run_bigquery_validation
        sql = """
        SELECT *
        FROM `bigquery-public-data.cms_synthetic_patient_data_omop.cost`
        LIMIT 5
        """
        result = run_bigquery_validation(sql, None)
        print("\nTest 4: SQL Validation")
        print("Successfully validated and executed SQL")
        print("Result:", result)
        return True
    except Exception as e:
        print(f"Error in test_run_bigquery_validation: {str(e)}")
        return False

if __name__ == "__main__":
    print("Starting tests...")
    
    # Run all tests
    tests = [
        test_bigquery_connection,
        test_get_database_settings,
        test_initial_bq_nl2sql,
        test_run_bigquery_validation
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__name__, result))
        except Exception as e:
            print(f"Error running {test.__name__}: {str(e)}")
            results.append((test.__name__, False))
    
    # Print summary
    print("\nTest Summary:")
    print("-" * 50)
    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        print(f"{test_name}: {status}") 