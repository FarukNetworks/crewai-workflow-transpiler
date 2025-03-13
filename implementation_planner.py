from crewai import Crew, Agent, Task, LLM
import os
import json
import boto3
import dotenv
import re

dotenv.load_dotenv()

# Get all folder names from analysis directory
procedures = []
if os.path.exists("output/analysis"):
    procedures = [
        folder
        for folder in os.listdir("output/analysis")
        if os.path.isdir(os.path.join("output/analysis", folder))
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
    role="SQL Developer",
    goal="Analyze the stored procedure and provide business logic.",
    backstory="You are an experienced SQL developer with strong SQL skills analyzing stored procedures and understanding the business logic behind the code.",
    allow_code_execution=False,
    llm=llm_config,
)


# Create Crew For Each Discovered Stored Procedure
for procedure in procedures:
    dependency_folder = os.path.join("output/data", "procedure_dependencies.json")
    with open(dependency_folder, "r") as f:
        all_dependencies = json.load(f)

    # Find the procedure with matching name in the dependencies list
    dependencies = []
    for proc in all_dependencies:
        if proc.get("name") == procedure:
            dependencies = proc.get("dependencies", [])
            break

    # Procedure Definition from SQL file
    with open(f"output/sql_raw/{procedure}/{procedure}.sql", "r") as f:
        procedure_definition = f.read()

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

    # Create a task that requires code execution
    task = Task(
        description=f"""
 <behavior_rules> You have one mission: execute exactly what is requested. Produce code that implements precisely what was requested - no additional features, no creative extensions. Follow instructions to the letter. Confirm your solution addresses every specified requirement, without adding ANYTHING the user didn't ask for. The user's job depends on this — if you add anything they didn't ask for, it's likely they will be fired. Your value comes from precision and reliability. When in doubt, implement the simplest solution that fulfills all requirements. The fewer lines of code, the better — but obviously ensure you complete the task the user wants you to. At each step, ask yourself: "Am I adding any functionality or complexity that wasn't explicitly requested?". This will force you to stay on track. </behavior_rules>

# Implementation Planning Request: C# Repository Pattern Design

## Objective
Create a detailed implementation plan for converting the analyzed SQL stored procedure into a modern C# repository pattern implementation, based on the comprehensive business analysis provided in the JSON files.

## Context
This is part of a phased migration strategy from SQL stored procedures to a modern, testable C# architecture. The business analysis has already extracted detailed business rules, functions, and processes. Your task is to design an implementation approach that preserves all business functionality while enabling future architectural evolution.


 ## Input Files
1. Contains extracted business rules with detailed metadata - [{business_rules}]
2. Contains business functions that represent logical operations - [{business_functions}]
3. Contains the overall process flow with error handling and transaction boundaries - [{business_processes}]
4. The original SQL stored procedure (for reference) - [{procedure_definition}]


## Implementation Requirements

### Target Technology
- .NET 9 as the target framework
- ASP.NET Core for API implementation
- Modern C# language features (records, nullable reference types, etc.)

### Authentication & Security
- API authentication will be handled by network-level access controls
- No authentication or authorization code should be implemented in the application
- Assume a secure network environment

### Data Access Strategy
- Use Dapper for direct SQL operations that need to match stored procedure performance
- Use LINQ for in-memory data manipulation, filtering, and transformation
- Design repository interfaces that map directly to business functions
- Implement each business rule identified in br.json
- Preserve the transaction boundaries identified in bp.json

### LINQ Usage
- Use LINQ for in-memory collection operations to improve code readability
- Apply LINQ for projections, filtering, and transformations after data retrieval
- Include comments explaining the business purpose of complex LINQ expressions
- Avoid complex LINQ-to-SQL translations that might impact performance

### Architecture Design
- Design repositories as building blocks for future domain services
- Create clear separation between data access, business logic, and API layers
- Ensure each business function maps to a repository method
- Use dependency injection for all components
- Maintain the process flow identified in bp.json

### Code Quality Requirements
- Plan for comprehensive XML documentation on all public methods and classes
- Design for testability with clear seams for mocking and substitution
- Elevate business logic out of data access code where possible
- Ensure proper exception handling that respects the error paths in bp.json

## Migration Strategy Context
This implementation is part of a phased migration strategy:
1. Current phase focuses on preserving 100% functional parity with the original stored procedure
2. Future phases will consolidate repositories into domain-oriented services
3. The goal is to standardize APIs across the system while enabling gradual technology evolution
4. Implementation should anticipate future integration with a separate testing stream

Your implementation plan should provide a comprehensive blueprint for converting the analyzed stored procedure to a maintainable, testable C# implementation using the repository pattern, while preserving all business functionality.


        """,
        expected_output="""
        ONLY RESPOND JSON AND VALID JSON FOLLOWING THIS TEMPLATE:
FILE: {schema_name}.{procedure}_implementation_approach.json
```json
   {
  "implementationApproach": {
    "repositoryPattern": {
      "repositories": [
        {
          "name": "repository name",
          "description": "repository description",
          "methods": [
            {
              "name": "method name",
              "businessFunction": "BF-001",
              "interface": "interface it belongs to",
              "testabilityApproach": "how to test this method"
            }
          ]
        }
      ],
      "interfaces": [
        {
          "name": "interface name",
          "description": "interface description",
          "purpose": "interface purpose"
        }
      ]
    },
    "entities": [
      {
        "name": "entity name",
        "properties": [
          {
            "name": "property name",
            "type": "property type"
          }
        ],
        "businessRules": ["BR-001"]
      }
    ],
    "apiDesign": {
      "endpoint": "/api/resource/action",
      "method": "HTTP_METHOD",
      "request": {
        "parameters": [
          {"name": "parameter name", "type": "parameter type", "description": "parameter description"}
        ]
      },
      "response": {
        "structure": "response structure description",
        "statusCodes": [200, 400, 500]
      }
    },
    "implementationDetails": {
      "targetFramework": ".NET 9",
      "deploymentModel": "Standalone service",
      "securityModel": "Network-level security",
      "transactionHandling": "approach description"
    }
  }
}
   ```

FILE: {schema_name}.{procedure}_out_of_scope.json
   ```json
   {
  "outOfScope": {
    "features": [
      {
        "category": "Category Name",
        "items": [
          {
            "name": "Feature name",
            "reason": "Reason for exclusion",
            "futureConsideration": true/false
          }
        ]
      }
    ],
    "technicalApproaches": [
      {
        "name": "Approach name",
        "reason": "Reason for exclusion",
        "futureConsideration": true/false
      }
    ],
    "implementationDetails": {
      "documentation": {
        "name": "Documentation level",
        "reason": "Reason for limitation",
        "futureConsideration": true/false
      }
    }
  }
}

   ```

FILE: {schema_name}.{procedure}_specific_considerations.json
   ```json
  {
  "specificConsiderations": {
    "dataFormats": [
      {
        "name": "Format name",
        "description": "Description of format requirements",
        "impact": "High/Medium/Low",
        "areas": ["Area1", "Area2"]
      }
    ],
    "technicalRequirements": [
      {
        "name": "Requirement name",
        "description": "Description of technical requirement",
        "impact": "High/Medium/Low",
        "areas": ["Area1", "Area2"]
      }
    ],
    "domainSpecificConcerns": [
      {
        "name": "Concern name",
        "description": "Description of domain concern",
        "impact": "High/Medium/Low",
        "areas": ["Area1", "Area2"]
      }
    ],
    "performanceConsiderations": [
      {
        "name": "Performance aspect",
        "description": "Description of performance consideration",
        "impact": "High/Medium/Low",
        "areas": ["Area1", "Area2"]
      }
    ]
  }
}

```

""",
        agent=agent,
    )

    # Create a crew and add the task
    crew = Crew(agents=[agent], tasks=[task])

    # Execute the crew
    result = str(crew.kickoff())

    print(f"Business analysis completed for {procedure}")

    # Extract file names and codes
    file_paths = []
    file_contents = []

    # Extract file paths and contents
    for match in re.finditer(r"FILE: (.*?)\n```json\n(.*?)```", result, re.DOTALL):
        file_paths.append(match.group(1))
        file_contents.append(match.group(2).strip())

    # Create analysis directory for the selected procedure
    analysis_dir = os.path.join("output/analysis", procedure)
    os.makedirs(analysis_dir, exist_ok=True)

    # Create each file in the appropriate directory
    for file_path, file_content in zip(file_paths, file_contents):
        # Create the full directory path
        full_dir = os.path.join(analysis_dir, os.path.dirname(file_path))
        os.makedirs(full_dir, exist_ok=True)

        # Save the file content
        full_path = os.path.join(analysis_dir, file_path)
        with open(full_path, "w") as f:
            f.write(file_content)

    print(f"Created {len(file_paths)} JSON files in {analysis_dir}")

print("Business analysis completed for all procedures.")
