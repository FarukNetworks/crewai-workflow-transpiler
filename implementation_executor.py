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
    procedures = [
        folder
        for folder in os.listdir("output/sql_raw")
        if os.path.isdir(os.path.join("output/sql_raw", folder))
    ]
print(f"Discovered procedures: {procedures}")


openai_config = LLM(model="gpt-4o", api_key=os.getenv("OPENAI_API_KEY"))

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
    api_key=os.getenv("ANTHROPIC_API_KEY"),
    timeout=600,  # Increase from default to at least 10 minutes (600 seconds)
    request_timeout=600,
    max_retries=2,
    max_tokens=64000,
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
    goal="Execute the implementation plan for the given stored procedure, included all other provided artefacts and create C# code ready for API layer following the repository pattern.",
    backstory="You are an experienced C# developer meticulously following the implementation plan. You are also an expert in C# and .NET 9. You are also an expert in the domain of the stored procedure.",
    allow_code_execution=False,
    llm=llm_config,
    verbose=True,
)

# Create Crew For Each Discovered Stored Procedure
for procedure in procedures:
    print(f"🔄 Generating csharp code for {procedure}")

    # business_rules
    with open(f"output/analysis/{procedure}/{procedure}_business_rules.json", "r") as f:
        business_rules = json.load(f)

    # business_functions
    with open(
        f"output/analysis/{procedure}/{procedure}_business_functions.json", "r"
    ) as f:
        business_functions = json.load(f)

    # business_processes
    with open(
        f"output/analysis/{procedure}/{procedure}_business_processes.json", "r"
    ) as f:
        business_processes = json.load(f)

    # implementation_approach
    with open(
        f"output/analysis/{procedure}/{procedure}_implementation_approach.json", "r"
    ) as f:
        implementation_approach = json.load(f)

    # out_of_scope
    with open(f"output/analysis/{procedure}/{procedure}_out_of_scope.json", "r") as f:
        out_of_scope = json.load(f)

    # specific_considerations
    with open(
        f"output/analysis/{procedure}/{procedure}_specific_considerations.json", "r"
    ) as f:
        specific_considerations = json.load(f)

    # procedure_definition
    with open(f"output/sql_raw/{procedure}/{procedure}.sql", "r") as f:
        procedure_definition = f.read()

    # Create a task that requires code execution
    task = Task(
        description=f"""
       <behavior_rules> You have one mission: execute exactly what is requested. Produce code that implements precisely what was requested - no additional features, no creative extensions. Follow instructions to the letter. Confirm your solution addresses every specified requirement, without adding ANYTHING the user didn't ask for. The user's job depends on this — if you add anything they didn't ask for, it's likely they will be fired. Your value comes from precision and reliability. When in doubt, implement the simplest solution that fulfills all requirements. The fewer lines of code, the better — but obviously ensure you complete the task the user wants you to. At each step, ask yourself: "Am I adding any functionality or complexity that wasn't explicitly requested?". This will force you to stay on track. </behavior_rules>

       ## Generic Prompt for Implementation Agent

```
# C# Implementation Request: Convert SQL Stored Procedure to Repository Pattern

## Background
I've completed a comprehensive analysis of a SQL stored procedure that has been decomposed into structured JSON files containing business rules, functions, processes, and implementation approaches. These files should guide your implementation work.

## Your Task
Create a C# implementation using the repository pattern that maintains full feature parity with the original stored procedure while making the code testable and maintainable. The implementation should follow modern C# practices and target .NET 9.


 ## Input Files
1. Contains extracted business rules with detailed metadata - [{business_rules}]
2. Contains business functions that represent logical operations - [{business_functions}]
3. Contains the overall process flow with error handling and transaction boundaries - [{business_processes}]
4. The original SQL stored procedure (for reference) - [{procedure_definition}]
5. The implementation approach for this procedure - [{implementation_approach}]
6. The out of scope items for this procedure - [{out_of_scope}]
7. The specific considerations for this procedure - [{specific_considerations}]
8. The original SQL stored procedure (for reference) - [{procedure_definition}]

## Required Deliverables
1. **C# Entity Classes**
   - Implement all entities defined in ia.json
   - Include proper properties, data types, and validation attributes

2. **Repository Interfaces**
   - Implement all interfaces defined in ia.json
   - Ensure method signatures match the described functionality

3. **Repository Implementations**
   - Implement concrete repositories that satisfy all interfaces
   - Ensure all business rules are applied as described in br.json
   - Use Dapper or Entity Framework Core for data access

4. **Service Layer**
   - Implement a service class that orchestrates the process described in bp.json
   - Ensure proper dependency injection

5. **API Controller**
   - Implement a REST API controller as described in ia.json
   - Include proper input validation and error handling

## Implementation Guidelines
1. Follow the repository pattern exactly as described in ia.json
2. Implement all business rules as described in br.json
3. Respect the process flow described in bp.json
4. Do NOT implement anything marked as out of scope in outOfScope.json
5. Address all specific considerations mentioned in specificConsiderations.json
6. Ensure 100% feature parity with the original stored procedure
7. Use modern C# practices (nullable reference types, records where appropriate, etc.)
8. Use dependency injection to maintain testability
9. Document the code with XML comments. 
10. Make sure XML comments contain references to business functions and business process from json files where applicable.
11. Use LINQ where appropriate for in-memory collection operations to improve code readability and maintainability

## Additional Information
- Review the specificConsiderations.json file carefully for any special handling required for this particular stored procedure
- The original stored procedure is provided as a reference to ensure feature parity
- If you have questions about specific business rules or implementation details, please ask for clarification

Please provide a complete implementation with all the required components.

Provide {procedure}.csproj file. 
Provide Program.cs file. 
Provide appsettings.json file with the following configuraiton 

{{
    "ConnectionStrings": {{
        "DefaultConnection": "Driver={connection_string}"
    }}
}}

        """,
        expected_output="""
        Return only the code with this format: 
        FILE: <folder_name>/<file_name>
        ```csharp
        <code>
        ``` 
        """,
        agent=agent,
    )

    # Create a crew and add the task
    crew = Crew(agents=[agent], tasks=[task])

    # Execute the crew
    result = str(crew.kickoff())

    print(f"C# code completed for {procedure}")

    # Extract file names and codes
    file_paths = []
    file_contents = []

    # Extract file paths and contents
    for match in re.finditer(r"FILE: (.*?)\n```csharp\n(.*?)```", result, re.DOTALL):
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
