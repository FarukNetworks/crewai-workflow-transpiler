from crewai import Crew, Agent, Task, LLM
import pyodbc
import questionary
from questionary import Choice
import dotenv
import os
import sqlparse
import json

dotenv.load_dotenv()

connection_string = os.getenv("CONNECTION_STRING")

connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

cursor.execute("""
SELECT 
    s.name + '.' + p.name AS name
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE p.type = 'P' 
AND p.name NOT LIKE 'tSQLt%'
ORDER BY s.name, p.name;""")
procedures = cursor.fetchall()

stored_procedures = []
for procedure in procedures:
    stored_procedures.append(procedure.name)

stored_procedures = [str(procedure) for procedure in stored_procedures]

def select_procedures():
    # Create choices with SELECT ALL option at the top
    procedure_choices = [
        Choice(title="SELECT ALL", value="SELECT_ALL")
    ] + [
        Choice(title=proc, value=proc) for proc in stored_procedures
    ]
    
    # Use checkbox selection (space to select, enter to confirm)
    selected = questionary.checkbox(
        "Select stored procedures (SPACE to select, ENTER to confirm):",
        choices=procedure_choices,
    ).ask()
    
    if not selected:
        print("No procedures selected. Exiting.")
        exit()
    
    # If SELECT ALL is chosen, return all procedures
    if "SELECT_ALL" in selected:
        return stored_procedures
        
    return selected

selected_procedures = select_procedures()
print(f"Selected procedures: {selected_procedures}")

openai_config = LLM(
    model="gpt-4o",
    api_key=os.getenv("OPENAI_API_KEY")
)

bedrock_config = LLM(
    model="bedrock/us.meta.llama3-3-70b-instruct-v1:0",
    max_retries=2,
    request_timeout=30,
    temperature=0.1,
    timeout=300,
    verbose=True,
)


anthropic_config = LLM(
    model="anthropic/claude-3-7-sonnet-20250219",
    api_key=os.getenv("ANTHROPIC_API_KEY")
)



llm_config = openai_config

# Create an agent with code execution enabled
coding_agent = Agent(
    role="SQL Developer",
    goal="Analyze data and provide meta data about the stored procedure.",
    backstory="You are an experienced SQL developer with strong SQL skills.",
    allow_code_execution=False,
    llm=llm_config
)

for procedure_name in selected_procedures:
    # Get the procedure definition
    cursor.execute(f"""SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID('{procedure_name}')""")
    procedure_definition = cursor.fetchone().definition
    # Remove SQL comments using sqlparse
    procedure_definition = sqlparse.format(procedure_definition, strip_comments=True).strip()

    # Save Definition to sql-raw folder 
    sql_dir = os.path.join("output/sql_raw", procedure_name)
    os.makedirs(sql_dir, exist_ok=True)

    # Save the result to a JSON file
    with open(os.path.join(sql_dir, f"{procedure_name}.sql"), "w") as f:
        f.write(procedure_definition)

    # Create a task that requires code execution
    data_analysis_task = Task(
        description=f"""
        Analyze the stored procedure `{procedure_name}` and provide the following metadata clearly structured in JSON:

        The goal of this task is to analyze a given SQL stored procedure and extract structured metadata, logical components, business rules, and test value candidates. The extracted data will be formatted into a predefined JSON schema, serving as an expected output reference for validation in an AI-driven workflow.

Task Steps
	1.	Metadata Extraction
	•	Identify the stored procedure name.
	•	Extract procedure parameters, including names, data types, and default values.
	2.	Logical Block Analysis
	•	Segment the stored procedure into logical code blocks (e.g., PROCEDURE_BODY, CONDITIONAL_LOGIC).
	•	Determine the role of each block (e.g., DATA_TRANSFORMATION, DATA_VALIDATION).
	3.	Table References Identification
	•	Identify all database tables used in the procedure.
	•	Extract JOIN conditions and referenced columns.
	4.	Business Rule Extraction
	•	Detect business rules within the procedure.
	•	Categorize rules based on integrity, validation, and transformation logic.
	5.	Statement-Level Analysis
	•	Classify statements based on their purpose (e.g., OBJECT_CREATION, DATA_RETRIEVAL).
	•	Capture conditional logic and constraints applied within the procedure.
	6.	Parameter Usage Assessment
	•	Determine how parameters are used in the procedure.
	•	Identify unused parameters or inconsistencies in parameter handling.
	7.	Test Value Candidates Generation
	•	Suggest test values for each parameter, ensuring edge case coverage.
	•	Provide test scenarios such as NULL_VALUE, BOUNDARY_CASE, and TYPICAL_CASE.

        Procedure Raw Code: {procedure_definition}
        """,
        expected_output="""
{
  "metadata": {
    "procedureName": "",
    "parameters": [{"name": "name", "dataType": "dataType", "defaultValue": "defaultValue"}]
  },
  "logicalBlocks": [
  {
  "id": "block_0",
      "type": "type",
      "lineRange": [lineRangeFrom, lineRangeTo],
      "codeText": "codeText",
      "comments": [],
      "childBlocks": [],
      "purpose": "purpose"
    }],
  "tableReferences": [{"tableName": "tableName", "columns": ["column1", "column2"]}],
  "potentialBusinessRules": [{"ruleType": "ruleType", "description": "description"}],
  "statementPurpose": [{"statementType": "statementType", "purpose": "purpose"}],
  "parameterUsage": [{"parameterName": "parameterName", "usage": "usage"}],
  "testValueCandidates": [{"parameterName": "parameterName", "testValues": ["testValue1", "testValue2"]}]
}""",
        agent=coding_agent
    )

    # Create a crew and add the task
    analysis_crew = Crew(
        agents=[coding_agent],
        tasks=[data_analysis_task]
    )

    # Execute the crew
    result = str(analysis_crew.kickoff())

    print(f"Analysis completed for {procedure_name}")

    # Create analysis directory for the selected procedure
    analysis_dir = os.path.join("output", "analysis", procedure_name)
    os.makedirs(analysis_dir, exist_ok=True)

    # Save the result to a JSON file
    with open(os.path.join(analysis_dir, f"{procedure_name}_meta.json"), "w") as f:
        f.write(result.replace("```json", "").replace("```", ""))


# Close the database connection
connection.close()
print("Analysis completed for all selected procedures.")