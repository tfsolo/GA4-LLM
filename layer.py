import pandas as pd
from google.cloud import bigquery
from fastmcp import FastMCP

# Setup the MCP Server and BigQuery Client
mcp = FastMCP("HiCoData Analytics")
client = bigquery.Client(project="hicodata-analytics")

@mcp.tool()
def run_ga4_sql(sql_query: str) -> str:
    """
    Executes a BigQuery SQL query. 
    IMPORTANT FOR AI: Always use the table `hicodata-analytics.analytics_525770274.events_*`.
    """
    try:
        # Run the AI-generated SQL query
        query_job = client.query(sql_query)
        
        # Convert the result to a DataFrame, then to a readable string
        df = query_job.to_dataframe()
        
        if df.empty:
            return "No data returned for this query."
            
        return df.to_string(index=False)
        
    except Exception as e:
        return f"SQL Error: {str(e)}"

if __name__ == "__main__":
    mcp.run()