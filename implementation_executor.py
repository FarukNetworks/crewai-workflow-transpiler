from crewai import Crew, Agent, Task, Process, LLM
from crewai_tools import FileWriterTool
from shared.get_dependencies import get_dependencies
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
    timeout=900,  # Increase to 15 minutes
    request_timeout=900,
    max_retries=3,
    max_tokens=64000,
)

if os.getenv("LLM_CONFIG") == "bedrock":
    llm_config = bedrock_config
elif os.getenv("LLM_CONFIG") == "anthropic":
    llm_config = anthropic_config
else:
    llm_config = openai_config

llm_config = openai_config

# Process each stored procedure
for procedure in procedures:
    print(f"🔄 Generating C# code for {procedure}")

    # Create output directory
    csharp_dir = os.path.join("output/csharp-code", procedure)
    os.makedirs(csharp_dir, exist_ok=True)

    # Load all the necessary JSON files
    with open(f"output/analysis/{procedure}/{procedure}_business_rules.json", "r") as f:
        business_rules = json.load(f)

    with open(
        f"output/analysis/{procedure}/{procedure}_business_functions.json", "r"
    ) as f:
        business_functions = json.load(f)

    with open(
        f"output/analysis/{procedure}/{procedure}_business_processes.json", "r"
    ) as f:
        business_processes = json.load(f)

    with open(
        f"output/analysis/{procedure}/{procedure}_implementation_approach.json", "r"
    ) as f:
        implementation_approach = json.load(f)

    with open(f"output/analysis/{procedure}/{procedure}_out_of_scope.json", "r") as f:
        out_of_scope = json.load(f)

    with open(
        f"output/analysis/{procedure}/{procedure}_specific_considerations.json", "r"
    ) as f:
        specific_considerations = json.load(f)

    with open(f"output/sql_raw/{procedure}/{procedure}.sql", "r") as f:
        procedure_definition = f.read()

    dependencies = get_dependencies(procedure)

    # Create a C# developer agent
    developer_agent = Agent(
        role="C# Developer",
        goal="Execute the implementation plan for the given stored procedure, creating a complete C# project following the repository pattern",
        backstory="""You are an experienced C# developer meticulously following implementation plans. 
        You are an expert in C# and .NET 9, with deep knowledge of the repository pattern and data access.
        You create clean, maintainable code with proper error handling and documentation.""",
        allow_code_execution=False,
        llm=llm_config,
        verbose=True,
    )

    # Task 1: Planning the implementation
    planning_task = Task(
        description=f"""
        TASK: Plan the C# implementation for the stored procedure {procedure}
        
        CONTEXT:
        You have been provided with a comprehensive analysis of a SQL stored procedure that has been decomposed into structured JSON files.
        
        AVAILABLE INFORMATION:
        1. Business rules: {json.dumps(business_rules, indent=2)}
        2. Business functions: {json.dumps(business_functions, indent=2)}
        3. Business processes: {json.dumps(business_processes, indent=2)}
        4. Implementation approach: {json.dumps(implementation_approach, indent=2)}
        5. Out of scope items: {json.dumps(out_of_scope, indent=2)}
        6. Specific considerations: {json.dumps(specific_considerations, indent=2)}
        7. Original SQL stored procedure: {procedure_definition}
        8. Dependencies: {dependencies}
        
        YOUR TASK:
        Create a detailed implementation plan that includes:
        1. A list of all C# files that need to be created, organized in appropriate folders
        2. The purpose and content of each file
        3. The dependencies between files
        4. The order in which files should be implemented
        
        FOLDER STRUCTURE:
        Follow a standard .NET project structure with folders like:
        - Models/ (for entity classes)
        - Repositories/ (for repository interfaces and implementations)
        - Services/ (for service layer classes)
        - Controllers/ (for API controllers)
        - DTOs/ (for data transfer objects)
        - Interfaces/ (for interfaces)
        - Helpers/ (for helper classes)
        
        Focus on creating a clean, maintainable implementation that follows the repository pattern and modern C# practices.
        """,
        expected_output="""
        A detailed implementation plan with a list of all files to be created (with their folder paths), their purpose, and the order of implementation.
        """,
        agent=developer_agent,
    )

    # Execute the planning task
    planning_result = planning_task.execute()
    print("✅ Implementation planning completed")

    # Task 2: Implementing the files one by one
    implementation_task = Task(
        description=f"""
        TASK: Implement the C# files according to the plan
        
        CONTEXT:
        You have created a detailed implementation plan for the stored procedure {procedure}.
        Now you need to implement each file one by one.
        
        INSTRUCTIONS:
        1. Follow the implementation plan from the previous task
        2. For each file, return ONLY the following format:
           
           FILE: <folder_name>/<file_name>
           ```csharp
           <code>
           ```
           
        3. Return ONLY ONE file at a time
        4. Wait for confirmation before proceeding to the next file
        5. Maintain consistency across all files
        6. Implement the .csproj file last, after all other files are created
        
        IMPORTANT NOTES:
        - Follow modern C# practices (nullable reference types, records where appropriate)
        - Use dependency injection
        - Document the code with XML comments
        - Reference business functions and processes in comments where applicable
        - Ensure 100% feature parity with the original stored procedure
        
        REQUIRED FILES AND FOLDERS:
        - Models/*.cs (entity classes)
        - Repositories/*.cs (repository interfaces and implementations)
        - Services/*.cs (service layer classes)
        - Controllers/*.cs (API controllers)
        - Program.cs (in root directory)
        - {procedure}.csproj (in root directory)
        - appsettings.json (in root directory) with connection string [{connection_string}] configuration
        """,
        expected_output="""
        Return only the code with this format: 
        FILE: <folder_name>/<file_name>
        ```csharp
        <code>
        ```
        """,
        agent=developer_agent,
    )

    # Create a crew with memory enabled and sequential process
    crew = Crew(
        agents=[developer_agent],
        tasks=[planning_task, implementation_task],
        process=Process.sequential,
        memory=True,  # Enable memory to maintain context between tasks
        verbose=True,
    )

    # Execute the crew to get the implementation plan
    crew.kickoff()

    # Extract file paths and contents from the implementation task result
    result = str(implementation_task.output)

    # Extract file paths and contents
    pattern = r"FILE:\s*([\w./\\-]+)\s*```(?:csharp|json|xml)\s*(.*?)```"
    matches = list(re.finditer(pattern, result, re.DOTALL))

    if not matches:
        print("⚠️ No files were found in the implementation result")
        continue

    # Process each file one by one
    for match in matches:
        file_path = match.group(1).strip()
        file_content = match.group(2).strip()

        # Skip empty paths or contents
        if not file_path or not file_content:
            continue

        try:
            # Clean the file path (remove any unexpected characters)
            clean_path = file_path.strip()

            # Create the full directory path
            full_dir = os.path.join(csharp_dir, os.path.dirname(clean_path))
            os.makedirs(full_dir, exist_ok=True)

            # Save the file content
            full_path = os.path.join(csharp_dir, clean_path)

            # Check if path is too long
            if len(full_path) > 255:  # Maximum path length on most systems
                print(f"⚠️ Path too long, truncating: {clean_path}")
                # Truncate the filename if needed
                base_dir = os.path.dirname(full_path)
                filename = os.path.basename(clean_path)
                if len(filename) > 50:
                    filename = filename[:45] + "..." + filename[-5:]
                full_path = os.path.join(base_dir, filename)

            with open(full_path, "w") as f:
                f.write(file_content)

            print(f"✅ Created file: {clean_path}")

        except Exception as e:
            print(f"❌ Error creating file {file_path}: {str(e)}")
            # Save to an error log file instead
            error_log_path = os.path.join(csharp_dir, "error_files.txt")
            with open(error_log_path, "a") as error_log:
                error_log.write(f"Error with file {file_path}: {str(e)}\n")
                error_log.write(f"Content:\n{file_content}\n\n")
                error_log.write("-" * 80 + "\n\n")

    # Now, let's implement the remaining files one by one using a separate task
    # Get the list of files that should be created based on the implementation plan
    required_file_types = [
        "Models/*.cs",
        "Repositories/*.cs",
        "Services/*.cs",
        "Controllers/*.cs",
        "Program.cs",
        f"{procedure}.csproj",
        "appsettings.json",
    ]

    # Check if we have all the required file types
    created_files = []
    for root, dirs, files in os.walk(csharp_dir):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, csharp_dir)
            created_files.append(relative_path)

    print(f"Created {len(created_files)} C# files in {csharp_dir}")
    for file in created_files:
        print(f"  - {file}")

# Close the database connection
connection.close()
print("✅ C# code completed for all procedures.")
