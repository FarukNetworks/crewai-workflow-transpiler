from crewai import Crew, Agent, Task, LLM
from shared.get_dependencies import get_dependencies
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
        and folder != "arm.stp_AddAuditRecord"
    ]
print(f"Discovered procedures: {procedures}")


openai_config = LLM(model="o3-mini", api_key=os.getenv("OPENAI_API_KEY"))

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
    goal="Analyze the stored procedure and provide integration Test Specification.",
    backstory="You are an experienced SQL developer with strong SQL skills analyzing stored procedures and understanding the business logic behind the code.",
    allow_code_execution=False,
    llm=llm_config,
)


# Create Crew For Each Discovered Stored Procedure
for procedure in procedures:
    # Read procedure definition from SQL file
    with open(f"output/sql_raw/{procedure}/{procedure}.sql", "r") as f:
        procedure_definition = f.read()

    dependencies = get_dependencies(procedure)

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
I'm migrating a SQL stored procedure to C# and need a comprehensive test suite to ensure feature parity. Please analyze the provided stored procedure and related files to create detailed test specifications in JSON format.

I've provided:
1. SQL stored procedure source code - [{procedure_definition}]
2. Business rules documentation - [{business_rules}]
3. Business functions documentation - [{business_functions}]
4. Business process documentation - [{business_processes}]
5. Dependencies information - [{dependencies}]

COVERAGE REQUIREMENTS:
1. Every business rule must have AT MINIMUM:
   - One test for the normal/expected case
   - Tests for ALL boundary conditions mentioned in the rule
   - Tests for ALL edge cases mentioned in the rule

2. Every business function must have:
   - A test validating the basic function operation
   - Tests for interactions between multiple rules within the function
   - Tests for any conditional logic or branching in the function

3. The overall process must have:
   - A test for the complete happy path flow
   - Tests for each alternative path or condition
   - Tests for error handling and exceptional conditions

4. Exploratory testing must include:
   - Tests for transaction behavior and rollback scenarios
   - Tests for SQL-specific behaviors that might behave differently in C#
   - Tests for implicit dependencies or assumptions in the code
   - Tests for NULL handling and edge data conditions
   - Tests for performance characteristics if relevant

For each identified test case, please create a separate JSON test specification that guarantees consistent test execution in both SQL and C# environments.

IMPORTANT: Each JSON object MUST contain:
1. EXACT test data with specific values for every field 
2. SPECIFIC configuration settings
3. CONCRETE validation criteria
4. For exploratory tests, include the "exploratoryReason" field (omit this field for non-exploratory tests)
5. Include only fields that are relevant to the specific test case (omit optional fields if not needed)
6. All actions must be explicitly stated (create, verify, update, delete, etc.)
7. All validation operations must be explicitly defined (exists, equals, etc.)

IMPORTANT: Make sure you adhere to exact data types for each entity. 
1. Dependencies file contains exact data types for each entity. 
2. You MUST use the exact data types for each entity. 
3. You MUST use the exact column names for each entity. 
4. You MUST use the exact data type for each column test data sample.
5. In [testDataSetup] every attributes value MUST match the exact data type of that attribute, and Values MUST be in the exact format of that data type.

IMPORTANT:
1. For now, give me one test for business rule, one for business function and one for business process. Add one exploratory test too. 

Please create separate JSON objects for all required test cases, ensuring COMPLETE COVERAGE of all business rules, functions, processes, and potential edge cases. For each business rule or function identified in the documentation, there must be at least one corresponding test specification.

Create one complete, valid JSON object per test case, and ensure it contains enough detail that both tSQLt and C# implementations would use IDENTICAL test data.


        """,
        expected_output="""
ONLY RESPOND IN JSON FORMAT  
Each JSON test specification should follow this structure:
```json
"testScenarios": [
{
  "testId": "A unique identifier",
  "type": "Quick or Thorough",
  "category": "BusinessRule, BusinessFunction, Process, or Exploratory",
  "ruleFunction": "For business rules/functions/processes: use exact identifier (BR-001, BF-003, PROC-001). For exploratory tests: use 'EXPL'",
  "exploratoryReason": "ONLY for exploratory tests: detailed explanation of why this test is needed",
  "description": "What aspect is being tested",
  "executionOrder": {
    "runAfter": ["Array of test IDs that must execute before this test"],
    "runBefore": ["Array of test IDs that must execute after this test"]
  },
  "testDataSetup": [
    {
      "entity": "Name of entity (e.g., Visit)",
      "identifier": "A unique identifier for this test entity",
      "action": "create|verify|update|delete",
      "dependsOn": [
        {"entity": "Related entity", "identifier": "ID of related entity", "relationship": "belongsTo|contains|references"}
      ],
      "attributes": {
        "attribute1": {"value": "exact value", "type": "SQL data type"},
        "attribute2": {"value": "exact value", "type": "SQL data type"}
      }
    }
  ],
  "systemConfiguration": [
    {
      "setting": "Configuration setting name",
      "action": "set|verify|delete",
      "value": "Exact value",
      "type": "SQL data type"
    }
  ],
  "testParameters": [
    {
      "name": "Parameter name",
      "action": "input",
      "value": "Exact value",
      "type": "SQL data type"
    }
  ],
  "dataVolume": {
    "size": "small|medium|large",
    "recordCount": "Number of records if applicable",
    "generationStrategy": "fixed|random"
  },
  "validationCriteria": [
    {
      "entity": "Entity to validate",
      "operation": "exists|notExists|equals|notEquals|greaterThan|lessThan|contains",
      "condition": "Exact condition to check",
      "expectedValue": "Precise expected value or result"
    }
  ],
  "expectedExceptions": {
    "shouldThrow": true|false,
    "exceptionType": "Type of exception expected",
    "messageContains": "Expected error message content"
  },
  "performanceCriteria": {
    "maxExecutionTimeMs": "Maximum acceptable execution time in milliseconds",
    "maxMemoryUsageMb": "Maximum acceptable memory usage in megabytes"
  },
  "cleanup": [
    {
      "entity": "Entity to clean up",
      "identifier": "Identifier of entity to remove or reset",
      "action": "delete|reset|restore"
    }
  ]
},
]
```

""",
        agent=agent,
    )

    # # Create a crew and add the task
    crew = Crew(agents=[agent], tasks=[task])

    # # Execute the crew
    result = str(crew.kickoff())

    # print(f"Integration test spec analysis completed for {procedure}")

    # # Create analysis directory for the selected procedure
    analysis_dir = os.path.join("output/analysis", procedure)
    os.makedirs(analysis_dir, exist_ok=True)

    # # Save the result to a JSON file
    with open(
        os.path.join(analysis_dir, f"{procedure}_integration_test_spec.json"), "w"
    ) as f:
        f.write(result.replace("```json", "").replace("```", ""))

    # Read the JSON file
    with open(
        os.path.join(analysis_dir, f"{procedure}_integration_test_spec.json"), "r"
    ) as f:
        integration_json_file = json.load(f)

    # Function to validate GUID format
    def is_valid_guid(guid):
        guid_pattern = re.compile(
            r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$"
        )
        return bool(guid_pattern.match(guid))

    # Function to fix an invalid GUID by replacing non-hex characters with '1'
    def fix_guid(guid):
        # Allow only hexadecimal characters and dashes
        fixed_guid = "".join(c if c in "0123456789ABCDEFabcdef-" else "1" for c in guid)
        return fixed_guid.upper()  # Return uppercase valid GUID

    # Iterate over test scenarios and fix invalid GUIDs
    for test in integration_json_file["testScenarios"]:
        for testDataSetup in test["testDataSetup"]:
            for attribute, details in testDataSetup["attributes"].items():
                if details["type"] == "uniqueidentifier":
                    original_guid = details["value"]
                    if not is_valid_guid(original_guid):
                        fixed_guid = fix_guid(original_guid)
                        print(
                            f"Invalid GUID found: {original_guid} → Fixed: {fixed_guid}"
                        )
                        details["value"] = fixed_guid  # Replace invalid GUID

    # Save the corrected JSON back to file
    with open(
        os.path.join(analysis_dir, f"{procedure}_integration_test_spec.json"), "w"
    ) as f:
        json.dump(integration_json_file, f, indent=4)

    print(
        "✅ JSON file processed. Invalid GUIDs have been corrected and saved to 'fixed_integration_json_file.json'."
    )
