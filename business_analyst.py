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
    model="anthropic/claude-3-7-sonnet-20250219", api_key=os.getenv("ANTHROPIC_API_KEY")
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
    # Read meta data from JSON file
    with open(f"output/analysis/{procedure}/{procedure}_meta.json", "r") as f:
        meta_data = json.load(f)

    with open(f"output/sql_raw/{procedure}/{procedure}.sql", "r") as f:
        sql_code = f.read()

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

    # Create a task that requires code execution
    task = Task(
        description=f"""
 <behavior_rules> You have one mission: execute exactly what is requested. Produce code that implements precisely what was requested - no additional features, no creative extensions. Follow instructions to the letter. Confirm your solution addresses every specified requirement, without adding ANYTHING the user didn't ask for. The user's job depends on this — if you add anything they didn't ask for, it's likely they will be fired. Your value comes from precision and reliability. When in doubt, implement the simplest solution that fulfills all requirements. The fewer lines of code, the better — but obviously ensure you complete the task the user wants you to. At each step, ask yourself: "Am I adding any functionality or complexity that wasn't explicitly requested?". This will force you to stay on track. </behavior_rules>


```
# Business Analysis Request: SQL Stored Procedure Decomposition

## Objective
Analyze the provided SQL stored procedure and decompose it into structured business components, rules, and processes to prepare for a modern C# implementation. The output should be detailed JSON files that document the business logic.

## Your Task
1. Thoroughly analyze the SQL stored procedure and any supporting files
2. Extract and document all business rules
3. Identify key business functions
4. Map the overall process flow, including branching and looping logic
5. Document any edge cases or special considerations

## Provided Files
1. The original SQL stored procedure [{procedure_definition}] -
2. Additional supporting files:
- DEPENDENCIES: [{dependencies}]

## Analysis Guidelines
1. Focus on the business intent rather than technical implementation
2. Identify implicit business rules that may not be explicitly documented
3. Note any areas where business logic seems unclear and would benefit from further clarification
4. Document any apparent data quality assumptions or edge case handling
5. Include confidence scores for your analysis when uncertainty exists
6. Use clear, consistent naming conventions throughout your analysis
7. Pay special attention to control flows:
   - Identify conditional branching (if/else logic)
   - Document looping constructs (foreach, while, etc.)
   - Note any parallel execution possibilities
   - Identify transaction boundaries and savepoints

## Control Flow Documentation
When documenting control flows, ensure you capture:

1. **Conditional Branches**: Points where the process takes different paths based on conditions
2. **Loops**: Processes that repeat for multiple records or until conditions are met
3. **Error Paths**: Alternative flows that execute when errors occur
4. **Early Termination**: Conditions that cause the process to end prematurely
5. **Dependencies**: Cases where one step must complete before another can begin

Please provide all outputs in the specified JSON format with appropriate nesting and relationships preserved.


        """,
        expected_output="""
        ONLY RESPOND JSON AND VALID JSON FOLLOWING THIS TEMPLATE:
FILE: {schema_name}.{procedure}_business_rules.json
```json
   {
     "businessRules": [
       {
         "id": "BR-001",
         "name": "Default MinsAfterFinal Value",
         "action": "Sets @MinsAfterFinal to 30 if NULL",
         "category": "configuration",
         "description": "If the 'MinsAfterFinal' parameter is not defined in the system, a default value of 30 minutes is used",
         "trigger": "Starting procedure execution when MinsAfterFinal parameter is NULL",
         "entities": ["MinsAfterFinal"],
         "implementation": {
           "sqlSnippet": "SELECT @MinsAfterFinal = CM.GetParameterValue('MinsAfterFinal')\nIF @MinsAfterFinal IS NULL SET @MinsAfterFinal = 30",
           "technicalDescription": "Retrieves 'MinsAfterFinal' value from system parameters and defaults to 30 if not found",
           "lineStart": 10,
           "lineEnd": 11
         },
         "confidenceScore": 95,
         "reasoning": "Explicit IF statement with default value assignment shows this is a clear configuration rule",
         "testValues": {
           "normalCases": ["GetParameterValue returns 45", "GetParameterValue returns NULL"],
           "boundaryCases": ["GetParameterValue returns 0"],
           "edgeCases": ["GetParameterValue returns negative value"]
         }
       },
       {
         "id": "BR-002",
         "name": "Hospital Inclusion Filter",
         "action": "Filters hospitals based on MeasureSetKey and Enabled status",
         "category": "validation",
         "description": "Only hospitals that are enabled for the specified MeasureSetKey should be included in processing",
         "trigger": "Procedure execution filtering hospitals for processing",
         "entities": ["Hospital", "MeasureSetKey"],
         "implementation": {
           "sqlSnippet": "SELECT DISTINCT HospitalID FROM CM.PeriodReadyForAbstraction \nWHERE MeasureSetKey = @MeasureSetKey AND Enabled = 1",
           "technicalDescription": "Filters hospitals by MeasureSetKey and Enabled status from PeriodReadyForAbstraction table",
           "lineStart": 17,
           "lineEnd": 19
         },
         "confidenceScore": 95,
         "reasoning": "Clear filtering of hospitals based on measure set and enabled status",
         "testValues": {
           "normalCases": ["MeasureSetKey with enabled hospitals", "MeasureSetKey with mix of enabled/disabled hospitals"],
           "boundaryCases": ["MeasureSetKey with no enabled hospitals"],
           "edgeCases": ["Invalid MeasureSetKey"]
         }
       }
     ]
   }
   ```

FILE: {schema_name}.{procedure}_business_functions.json
   ```json
   {
     "businessFunctions": [
       {
         "id": "BF-001",
         "name": "Initialize Configuration Parameters",
         "description": "Fetches and initializes configuration parameters required for the measurement process, including MinsAfterFinal and IncludedFacilityIDs",
         "rules": ["BR-001", "BR-003"],
         "repositoryMethod": {
           "name": "InitializeConfigurationParameters",
           "returnType": "ConfigurationParameters",
           "parameters": [
             {
               "name": "measureSetKey",
               "type": "int",
               "description": "The key identifying the measurement set being processed"
             }
           ]
         },
         "dataRequirements": {
           "inputs": ["MeasureSetKey"],
           "outputs": ["MinsAfterFinal", "IncludedFacilityIDs"],
           "dependencies": ["CM.GetParameterValue"]
         }
       },
       {
         "id": "BF-002",
         "name": "Determine Eligible Hospitals",
         "description": "Identifies hospitals that are eligible for processing based on the measurement set and enabled status",
         "rules": ["BR-002"],
         "repositoryMethod": {
           "name": "GetEligibleHospitals",
           "returnType": "IEnumerable<int>",
           "parameters": [
             {
               "name": "measureSetKey",
               "type": "int",
               "description": "The key identifying the measurement set being processed"
             },
             {
               "name": "hospitalId",
               "type": "int?",
               "description": "Optional hospital ID to filter results to a specific hospital"
             }
           ]
         },
         "dataRequirements": {
           "inputs": ["MeasureSetKey", "HospitalID"],
           "outputs": ["List of eligible Hospital IDs"],
           "dependencies": ["CM.PeriodReadyForAbstraction"]
         }
       }
     ]
   }
   ```

FILE: {schema_name}.{procedure}_business_processes.json
   ```json
   {
     "businessProcesses": [
       {
         "id": "PROC-001",
         "name": "Initial Patient Population Processing",
         "description": "Processes patient visits to determine and record the Initial Patient Population (IPP) for a measurement set, including creating detailed snapshots with diagnosis and procedure data",
         "orchestration": {
           "steps": [
             {
               "id": "STEP-001",
               "sequence": 1,
               "functionId": "BF-001",
               "type": "configurationRetrieval",
               "description": "Retrieve and initialize configuration parameters",
               "inputs": ["MeasureSetKey"],
               "outputs": ["MinsAfterFinal", "IncludedFacilityIDs"],
               "businessRules": ["BR-001", "BR-003"],
               "controlFlow": {
                 "type": "standard",
                 "nextStep": "STEP-002"
               }
             },
             {
               "id": "STEP-002",
               "sequence": 2,
               "functionId": "BF-002",
               "type": "dataRetrieval",
               "description": "Determine eligible hospitals for processing",
               "inputs": ["MeasureSetKey", "HospitalID"],
               "outputs": ["List of eligible Hospital IDs"],
               "businessRules": ["BR-002"],
               "controlFlow": {
                 "type": "conditional",
                 "condition": "Eligible hospitals exist",
                 "nextStepIfTrue": "STEP-003",
                 "nextStepIfFalse": "STEP-END"
               }
             },
             {
               "id": "STEP-003",
               "sequence": 3,
               "functionId": "BF-003",
               "type": "dataProcessing",
               "description": "Process visits using hospital list",
               "inputs": ["HospitalList"],
               "outputs": ["ProcessedVisits"],
               "businessRules": ["BR-004"],
               "controlFlow": {
                 "type": "loop",
                 "loopType": "forEach",
                 "loopCollection": "List of eligible Hospital IDs",
                 "maxIterations": 500,
                 "exitCondition": "All hospitals processed",
                 "nextStepAfterLoop": "STEP-END"
               }
             },
             {
               "id": "STEP-END",
               "sequence": 4,
               "functionId": null,
               "type": "completion",
               "description": "End of process",
               "inputs": [],
               "outputs": [],
               "businessRules": []
             }
           ],
           "errorPaths": [
             {
               "errorType": "Configuration Error",
               "sourceStep": "STEP-001",
               "handling": "Log error and stop processing",
               "recoveryStep": null,
               "abortProcess": true
             },
             {
               "errorType": "Data Access Error",
               "sourceStep": "STEP-002",
               "handling": "Log error, attempt retry, and then stop processing if persistent",
               "recoveryStep": "STEP-002",
               "abortProcess": true
             }
           ],
           "transactionBoundaries": {
             "startStep": "STEP-001",
             "commitStep": "STEP-END",
             "savepoints": [
               {
                 "atStep": "STEP-002",
                 "name": "AfterConfiguration"
               }
             ],
             "rollbackCondition": "Any critical error during processing that compromises data integrity",
             "rollbackToSavepoint": "AfterConfiguration"
           }
         }
       }
     ]
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
