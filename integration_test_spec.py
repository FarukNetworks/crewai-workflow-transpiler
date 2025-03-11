from crewai import Crew, Agent, Task, LLM
import os
import json
import boto3
import dotenv

dotenv.load_dotenv()

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


llm_config = openai_config

# Create a coding agent
agent = Agent(
    role="SQL Developer",
    goal="Analyze the stored procedure and provide integration Test Specification.",
    backstory="You are an experienced SQL developer with strong SQL skills analyzing stored procedures and understanding the business logic behind the code.",
    allow_code_execution=False,
    llm=llm_config
)


# Create Crew For Each Discovered Stored Procedure 
for procedure in procedures:
    # Read meta data from JSON file
    with open(f"output/analysis/{procedure}/{procedure}_meta.json", "r") as f:
        meta_data = json.load(f)
    
    # Create a task that requires code execution
    task = Task(
        description=f"""
        Analyze the stored procedure `{procedure}` and provide the following metadata clearly structured in JSON:

   ## Purpose

You are tasked with creating a comprehensive integration test specification in JSON format that will be used to verify feature parity between a SQL stored procedure and its C# repository pattern implementation.

## Inputs

You have been provided with:

1. A **Technical JSON specification** containing:
   - Detailed business process definitions
   - Data models and entity relationships
   - Transaction boundaries
   - Query patterns and business rules
   - Input parameter specifications

2. The original **SQL stored procedure code**

## Your Task

Create a detailed JSON specification for integration tests that will verify complete feature parity between the SQL stored procedure and C# implementation. The specification should define all aspects of the tests without providing actual implementation code.

## Coverage Requirements

1. **Full Process Coverage**: Include scenarios that test every business process identified in the technical JSON

2. **Complete Workflow Verification**: Each scenario should verify the entire workflow, not just individual components

3. **End-to-End Testing**: Focus on testing complete business processes rather than isolated business rules

4. **Data Variation**: Include specifications for different data conditions to thoroughly test the procedure

5. **Parameter Variations**: Specify tests for all meaningful parameter combinations

6. **Edge Cases**: Include scenario variations for boundary conditions

7. **Feature Parity Focus**: Every test scenario should include specific verification steps to confirm identical behavior between SQL and C# implementations

## Test Scenario Generation Guidelines

1. **Process-Driven**: Base test scenarios on the business processes identified in the technical JSON

2. **Data-Dependent Cases**: Identify key data conditions that affect behavior

3. **Parameter Space**: Create variations based on different parameter combinations

4. **Verification Points**: Define specific points to verify database state and outputs

5. **Exceptional Flows**: Include scenarios that test error handling and edge cases

## Final Verification Steps

Before submitting your specification, verify:

1. Have you included scenarios for every business process in the technical JSON?
2. Does each scenario have clear verification steps for feature parity?
3. Have you defined the required test data for each scenario?
4. Does the coverage matrix demonstrate complete coverage of processes and parameters?
5. Are the test data scenarios sufficient to test all key data conditions?

Your output should be a valid, well-structured JSON document that follows the schema above, with detailed specifications for comprehensive integration testing.



    META DATA: {meta_data}
        """,
        expected_output="""
   Your output should follow this structure:

```json
{
  "integrationTestSpecification": {
    "procedureName": "Schema.ProcedureName",
    "description": "Overall description of the procedure's purpose",
    "testEnvironmentRequirements": {
      "databaseObjects": [
        {
          "type": "table/view/procedure",
          "name": "ObjectName",
          "required": true,
          "purpose": "How this object is used in testing"
        }
      ],
      "externalDependencies": [
        {
          "name": "DependencyName",
          "type": "Configuration/Service/Function",
          "purpose": "How this dependency is used"
        }
      ],
      "dataResetStrategy": {
        "approach": "Description of recommended reset approach",
        "tables": ["Tables that need resetting between tests"]
      }
    },
    "testDataRequirements": {
      "entities": [
        {
          "entityName": "EntityName",
          "baseProperties": {
            "Property1": "default value",
            "Property2": "default value" 
          },
          "relationships": [
            {
              "relatedEntity": "RelatedEntityName",
              "relationship": "one-to-many/many-to-one",
              "required": true
            }
          ],
          "scenarios": [
            {
              "scenarioName": "Description of data variation",
              "propertyOverrides": {
                "Property1": "scenario-specific value"
              }
            }
          ]
        }
      ],
      "dataVolumes": {
        "minimum": "Minimum data volume for valid testing",
        "recommended": "Recommended data volume for thorough testing" 
      }
    },
    "testScenarios": [
      {
        "scenarioId": "SCEN-001",
        "name": "Descriptive name of scenario",
        "businessProcesses": ["Business process IDs from technical JSON"],
        "description": "Detailed description of what this scenario tests",
        "inputs": {
          "parameters": [
            {
              "name": "ParameterName",
              "value": "Parameter value or expression",
              "purpose": "Why this value is chosen"
            }
          ],
          "testData": [
            {
              "entity": "EntityName",
              "scenario": "ScenarioName or custom specification",
              "count": "Number of entities needed",
              "customCriteria": "Any specific criteria for this test"
            }
          ]
        },
        "execution": {
          "executionOrder": ["Steps to execute"],
          "transactionHandling": "How transactions should be handled"
        },
        "verification": {
          "databaseState": [
            {
              "table": "TableName",
              "verificationQuery": "Query to retrieve results",
              "expectedResults": {
                "type": "rowCount/specificValues/dataShape",
                "details": "Expected outcome details"
              }
            }
          ],
          "outputValidation": [
            {
              "outputType": "returnValue/resultSet/sideEffect",
              "validation": "How to validate this output"
            }
          ],
          "exceptionalCases": [
            {
              "condition": "When this condition occurs",
              "expectedBehavior": "How the system should behave"
            }
          ]
        },
        "variations": [
          {
            "variationId": "VAR-001",
            "description": "Description of this variation",
            "parameterChanges": {"Parameter": "New value"},
            "expectedDifferences": "How results should differ"
          }
        ]
      }
    ],
    "parityValidation": {
      "comparisonApproach": "How to compare SQL and C# results",
      "tolerances": {
        "numericValues": "Accepted tolerance for numeric differences",
        "timing": "Accepted tolerance for timing differences"
      },
      "reconciliationStrategy": "How to handle and report differences"
    },
    "testExecutionSequence": [
      {
        "phase": "Phase name",
        "scenarios": ["SCEN-001", "SCEN-002"],
        "dependsOn": "Previous phase if applicable"
      }
    ]
  },
  "coverageMatrix": {
    "businessProcesses": [
      {
        "processId": "PROC-001",
        "scenarios": ["SCEN-001", "SCEN-002"],
        "coveragePercentage": 100
      }
    ],
    "parameterCombinations": [
      {
        "parameters": "Description of parameters tested",
        "scenarios": ["SCEN-001", "VAR-001"]
      }
    ],
    "dataConditions": [
      {
        "condition": "Description of data condition",
        "scenarios": ["SCEN-002"]
      }
    ]
  },
  "testDataScenarios": [
    {
      "scenarioName": "ScenarioName",
      "description": "Description of this data scenario",
      "entities": [
        {
          "entityName": "EntityName",
          "count": "Number of entities",
          "properties": {
            "Property1": "Value or range",
            "Property2": "Value or range"
          }
        }
      ],
      "relationships": [
        {
          "description": "Description of relationship setup"
        }
      ]
    }
  ]
}

}""",
        agent=agent
    )

    # Create a crew and add the task
    crew = Crew(
        agents=[agent],
        tasks=[task]
    )

    # Execute the crew
    result = str(crew.kickoff())

    print(f"Integration test spec analysis completed for {procedure}")

    # Create analysis directory for the selected procedure
    analysis_dir = os.path.join("output/analysis", procedure)
    os.makedirs(analysis_dir, exist_ok=True)

    # Save the result to a JSON file
    with open(os.path.join(analysis_dir, f"{procedure}_integration_test_spec.json"), "w") as f:
        f.write(result.replace("```json", "").replace("```", ""))

print("Integration test spec analysis completed for all procedures.")