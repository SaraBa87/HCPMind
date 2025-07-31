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

"""Database Agent: get data from database (BigQuery) using NL2SQL."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.genai import types
from google.adk.tools.google_api_tool import CalendarToolset

from . import tools
# from .chase_sql import chase_db_tools
# from .prompts import return_instructions_bigquery

NL2SQL_METHOD = os.getenv("NL2SQL_METHOD", "BASELINE")


def setup_before_agent_call(callback_context: CallbackContext) -> None:
    """Setup the agent."""

    if "database_settings" not in callback_context.state:
        callback_context.state["database_settings"] = \
            tools.get_database_settings()

import os


def return_instructions_bigquery() -> str:

    
    instruction_prompt_bqml_v1 = f"""
      You are an AI assistant serving as a SQL expert for BigQuery.
      Your job is to help users generate SQL answers from natural language questions (inside Nl2sqlInput).
      You should proeuce the result as NL2SQLOutput.

      Use the provided tools to help generate the most accurate SQL:
      1. First, use initial_bq_nl2sql tool to generate initial SQL from the question.
      2. You should also validate the SQL you have created for syntax and function errors (Use run_bigquery_validation tool). If there are any errors, you should go back and address the error in the SQL. Recreate the SQL based by addressing the error.
      3. Generate the final result in JSON format with four keys: "explain", "sql", "sql_results", "nl_results".
          "explain": "write out step-by-step reasoning to explain how you are generating the query based on the schema, example, and question.",
          "sql": "Output your generated SQL!",
          "sql_results": "raw sql execution query_result from run_bigquery_validation if it's available, otherwise None",
          "nl_results": "Natural language about results, otherwise it's None if generated SQL is invalid"
      4. Ask user if they want to see the results summary or table.
      5. If they want to see the results summary, use display_results_summary tool to display the results summary.
      6. If they want to see the results as a table, use display_results_as_table tool to display the results as a table.
      ```
      You should pass one tool call to another tool call as needed!

      NOTE: you should ALWAYS USE THE TOOLS (initial_bq_nl2sql AND run_bigquery_validation) to generate SQL, not make up SQL WITHOUT CALLING TOOLS.
      Keep in mind that you are an orchestration agent, not a SQL expert, so use the tools to help you generate SQL, but do not make up SQL.

    """

    return instruction_prompt_bqml_v1

def return_instructions_cost_effectiveness() -> str:
    instruction_prompt_cost_effectiveness = f"""
    You are a sub-agent that is responsible for evaluating cost-effectiveness of providers and classify them based on quality vs cost.

    You should use the provided tools to help you generate the most accurate SQL:
    1. First, use initial_bq_nl2sql tool to generate initial SQL from the question.
    2. You should also validate the SQL you have created for syntax and function errors (Use run_bigquery_validation tool). If there are any errors, you should go back and address the error in the SQL. Recreate the SQL based by addressing the error.
    3. Generate the final result in JSON format with four keys: "explain", "sql", "sql_results", "nl_results".
    4. to calculate cost-effectiveness, you should follow the following steps:
        1. Calculate drug cost per capita per provider
        2. calculate procedure cost per capita per provider
        3. calculate total cost per capita per provider
        4. calculate percentage of popuplation over 50 years old per provider
        5. calculate percentage of female population per provider
        6. calculate percentage of diabetics per provider
        7. calculate risk profile per provider
        8. calculate cost-effectiveness score per provider
        9. classify the provider based on the cost-effectiveness score
        10. You just need to return provider ID, Risk Profile and Cost-Effectiveness Score
        11. Use plot tool to plot the results. 

    """
    return instruction_prompt_cost_effectiveness
# cost_agent = Agent(
#     model=os.getenv("BIGQUERY_AGENT_MODEL"),
#     name="cost_agent",
#     instruction=return_instructions_bigquery(),
#     tools=[tools.initial_bq_nl2sql, tools.run_bigquery_validation,
#     ],
# )


root_agent = Agent(
    model=os.getenv("BIGQUERY_AGENT_MODEL"),
    name="database_agent",
    instruction=return_instructions_bigquery(),
    tools=[tools.initial_bq_nl2sql, tools.run_bigquery_validation,
           tools.display_results_summary, tools.display_results_as_table
    ],
    before_agent_callback=setup_before_agent_call,
    generate_content_config=types.GenerateContentConfig(temperature=0.01),
)
