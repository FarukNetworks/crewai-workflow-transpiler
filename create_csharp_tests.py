from crewai import Crew, Agent, Task, LLM
import os
import json
import boto3
import dotenv
import sqlparse
import pyodbc
import re

dotenv.load_dotenv()

connection_string = os.getenv("CONNECTION_STRING")

connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

# Get all folder names from analysis directory
procedures = []
if os.path.exists("output/analysis"):
    procedures = [folder for folder in os.listdir("output/analysis") if os.path.isdir(os.path.join("output/analysis", folder))]
print(f"Discovered procedures: {procedures}")


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


if os.getenv("LLM_CONFIG") == "bedrock":
    llm_config = bedrock_config
elif os.getenv("LLM_CONFIG") == "anthropic":
    llm_config = anthropic_config
else:
    llm_config = openai_config

# Create a coding agent
agent = Agent(
    role="C# Unit Test Developer",
    goal="Analyze the stored procedure test code and provide C# unit test code.",
    backstory="You are an experienced C# developer with strong C# skills analyzing stored procedures unit test code and creating the same logic in C# unit test code.",
    allow_code_execution=False,
    llm=llm_config,
    verbose=True,
)




# Create Crew For Each Discovered Stored Procedure 
for procedure in procedures:
    print(f"🔄 Generating new test for {procedure}")
    # Read meta data from JSON file
    with open(f"output/analysis/{procedure}/{procedure}_meta.json", "r") as f:
        meta_data = json.load(f)
    
    # Read business logic from JSON file - using a more robust approach
    try:
        with open(f"output/analysis/{procedure}/{procedure}_business_logic.json", "r") as f:
            content = f.read()
            # Find the last closing brace of the JSON object
            json_end = content.rstrip().rfind('}')
            if json_end != -1:
                valid_json = content[:json_end+1]
                business_logic = json.loads(valid_json)
            else:
                # Fallback if no closing brace is found
                business_logic = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"⚠️ Error parsing business logic JSON for {procedure}: {e}")
        print(f"Attempting to fix the JSON format...")
        try:
            with open(f"output/analysis/{procedure}/{procedure}_business_logic.json", "r") as f:
                content = f.read()
                # Try to find a valid JSON object by looking for balanced braces
                open_braces = 0
                close_braces = 0
                for i, char in enumerate(content):
                    if char == '{':
                        open_braces += 1
                    elif char == '}':
                        close_braces += 1
                        if open_braces == close_braces:
                            # We found a balanced JSON object
                            valid_json = content[:i+1]
                            business_logic = json.loads(valid_json)
                            break
                else:
                    print(f"❌ Could not fix JSON format for {procedure}")
                    continue
        except Exception as e:
            print(f"❌ Failed to parse business logic JSON for {procedure}: {e}")
            continue

    # Read integration test spec from JSON file
    try:
        with open(f"output/analysis/{procedure}/{procedure}_integration_test_spec.json", "r") as f:
            integration_test_spec = json.load(f)
    except json.JSONDecodeError as e:
        print(f"⚠️ Error parsing integration test spec JSON for {procedure}: {e}")
        # Apply the same fix as for business logic
        try:
            with open(f"output/analysis/{procedure}/{procedure}_integration_test_spec.json", "r") as f:
                content = f.read()
                json_end = content.rstrip().rfind('}')
                if json_end != -1:
                    valid_json = content[:json_end+1]
                    integration_test_spec = json.loads(valid_json)
                else:
                    print(f"❌ Could not fix JSON format for {procedure}")
                    continue
        except Exception as e:
            print(f"❌ Failed to parse integration test spec JSON for {procedure}: {e}")
            continue

    # Read Unit Test 
    try:
        with open(f"output/sql-tests/{procedure}/{procedure}_test.sql", "r") as f:
            unit_test_code = f.read()
    except Exception as e:
        print(f"❌ Failed to read SQL test file for {procedure}: {e}")
        continue

    # Create a task that requires code execution
    task = Task(
        description=f"""
        Analyze the stored procedure `{procedure}` and provide C# Unit Test code

        The tests should cover:

        - All the tests from tSQLt into C# but for API layer transpiled with methods using repository pattern
        - All the mocking objects from tSQLt into C# 

        Structure tests clearly and provide scripts ready to execute.

        SQL UNIT TEST CODE: {unit_test_code} 
        BUSINESS LOGIC: {business_logic}
        """,
        expected_output="""
        csharp unit test code without any comments. Return only the code.
        """,
        agent=agent
    )

    # Create a crew and add the task
    crew = Crew(
        agents=[agent],
        tasks=[task]
    )

    # Execute the crew
    result = str(crew.kickoff())

    print(f"C# Unit Test code completed for {procedure}")
    result = result.replace("```csharp", "").replace("```", "")

    # Create analysis directory for the selected procedure
    test_dir = os.path.join("output/csharp-tests", procedure)
    test_file_path = os.path.join(test_dir, f"{procedure}_test.cs")
    os.makedirs(test_dir, exist_ok=True)

    # Save the result to a C# file
    with open(test_file_path, "w") as f:
        f.write(result)
    
    print(f"✅ C# Unit Test code completed for {procedure}")

    
print("✅ C# Unit Test code completed for all procedures.")
