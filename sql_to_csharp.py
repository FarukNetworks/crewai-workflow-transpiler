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
if os.path.exists("output/sql_raw"):
    procedures = [folder for folder in os.listdir("output/sql_raw") if os.path.isdir(os.path.join("output/sql_raw", folder))]
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
    role="C# Developer",
    goal="Analyze the stored procedure test code and provide C# code ready for API layer transpiled with repository pattern.",
    backstory="You are an experienced C# developer with strong C# skills analyzing stored procedures unit test code and creating the same logic in C# unit test code.",
    allow_code_execution=False,
    llm=llm_config,
    verbose=True,
)

# Create Crew For Each Discovered Stored Procedure 
for procedure in procedures:
    print(f"🔄 Generating new test for {procedure}")
    # Read meta data from JSON file
    try:
        with open(f"output/analysis/{procedure}/{procedure}_meta.json", "r") as f:
            meta_data = json.load(f)
    except Exception as e:
        print(f"⚠️ Error reading meta data for {procedure}: {e}")
        meta_data = {}
    
    # Read business logic from JSON file
    try:
        with open(f"output/analysis/{procedure}/{procedure}_business_logic.json", "r") as f:
            business_logic = json.load(f)
    except Exception as e:
        print(f"⚠️ Error reading business logic for {procedure}: {e}")
        business_logic = {}

    # Read integration test spec from JSON file
    try:
        with open(f"output/analysis/{procedure}/{procedure}_integration_test_spec.json", "r") as f:
            integration_test_spec = json.load(f)
    except Exception as e:
        print(f"⚠️ Error reading integration test spec for {procedure}: {e}")
        integration_test_spec = {}

    # Read Unit Test (if it exists)
    unit_test_code = ""
    if os.path.exists(f"output/csharp-tests/{procedure}/{procedure}_test.cs"):
        try:
            with open(f"output/csharp-tests/{procedure}/{procedure}_test.cs", "r") as f:
                unit_test_code = f.read()
        except Exception as e:
            print(f"⚠️ Error reading C# test file for {procedure}: {e}")
    else:
        print(f"ℹ️ No C# test file found for {procedure}, continuing without test code")

    # Stored Procedure Code 
    try:
        with open(f"output/sql_raw/{procedure}/{procedure}.sql", "r") as f:
            stored_procedure_code = f.read()
    except Exception as e:
        print(f"❌ Failed to read SQL stored procedure file for {procedure}: {e}")
        continue

    # Create a task that requires code execution
    task = Task(
        description=f"""
        Analyze the stored procedure `{procedure}` and create a project with C# code ready for API layer transpiled with repository pattern.

        The code should follow the TDD:

        - C# unit tests: {unit_test_code}
        - Business Logic: {business_logic}
        - Stored Procedure SQL CODE: {stored_procedure_code}

        In business logic JSON file you already have recommendation for implementation in C#. 
        Follow the rules in business logic JSON file.
        DO NOT USE INLINE SQL CODE. 

        Do not create tests folder or unit test files.
        Transpile the stored procedure to C# code ready for API layer transpiled with repository pattern.
        """,
        expected_output="""
        csharp code without any comments. Return only the code with this format: 
        FILE: <folder_name>/<file_name>
        ```csharp
        <code>
        ``` 
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

    print(f"C# code completed for {procedure}")
    
    # Extract file names and codes
    file_paths = []
    file_contents = []
    
    # Extract file paths and contents
    for match in re.finditer(r'FILE: (.*?)\n```csharp\n(.*?)```', result, re.DOTALL):
        file_paths.append(match.group(1))
        file_contents.append(match.group(2).strip())
    
    # Create analysis directory for the selected procedure
    csharp_dir = os.path.join("output/csharp-code", procedure)
    os.makedirs(csharp_dir, exist_ok=True)
    
    # Create each file in the appropriate directory
    for file_path, file_content in zip(file_paths, file_contents):
        # Create the full directory path
        full_dir = os.path.join(csharp_dir, os.path.dirname(file_path))
        os.makedirs(full_dir, exist_ok=True)
        
        # Save the file content
        full_path = os.path.join(csharp_dir, file_path)
        with open(full_path, "w") as f:
            f.write(file_content)
    
    print(f"Created {len(file_paths)} C# files in {csharp_dir}")
    

    
print("✅ C# code completed for all procedures.")
