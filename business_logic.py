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
    goal="Analyze the stored procedure and provide business logic.",
    backstory="You are an experienced SQL developer with strong SQL skills analyzing stored procedures and understanding the business logic behind the code.",
    allow_code_execution=False,
    llm=llm_config
)


# Create Crew For Each Discovered Stored Procedure 
for procedure in procedures:
    # Read meta data from JSON file
    with open(f"output/analysis/{procedure}/{procedure}_meta.json", "r") as f:
        meta_data = json.load(f)
    
    with open(f"output/sql_raw/{procedure}/{procedure}.sql", "r") as f:
        sql_code = f.read()
    # Create a task that requires code execution
    task = Task(
        description=f"""
        # Comprehensive SQL Stored Procedure Analysis for Repository Pattern Migration

```
You are tasked with analyzing a SQL stored procedure and its associated analysis metadata to extract both technical details and business-friendly descriptions of the embedded business rules, functions, and processes. This analysis will serve as the foundation for migrating the stored procedure to a C# repository pattern implementation with feature parity, using a phased migration approach.

## Input Files
You will be provided with two files:
1. The SQL stored procedure code file
2. An analysis JSON file containing metadata and parsed information about the procedure

## Deliverables
Produce two JSON files:

## Analysis Guidelines

1. **Rule Extraction**: Identify distinct business rules from the SQL code by looking for filtering conditions, calculations, validations, temporal constraints, etc. Group SQL operations that together implement a single business concept.

2. **Function Identification**: Group related rules into logical business functions that represent discrete operations or capabilities that would map to repository methods.

3. **Process Mapping**: Identify the overall business process and its steps, showing how the functions work together.

4. **Entity Recognition**: Identify key data entities referenced in the procedure and their relationships to inform C# class design.

5. **Technical Context**: Analyze the code for important technical characteristics and prioritize them based on business impact. Consider:
   - Hard-coded business rules that might need to change
   - Complex logic patterns that affect eligibility or classification
   - Time-sensitive processing constraints
   - Data formatting considerations (especially for regulatory requirements)
   - Transaction and performance implications

6. **Repository Pattern Focus**: Pay special attention to aspects that will inform the C# repository implementation:
   - Transaction boundaries and scopes
   - External dependencies that will need injection
   - State management approaches
   - Error handling patterns
   - Large dataset operations
   - Query optimization patterns
   - Concurrency handling
   - Bulk operations
   - Caching opportunities
   - SQL Server-specific functions and features
   - Data projection patterns
   - Dynamic SQL usage
   - Cross-cutting concerns
   - Repository interface design
   
7. **Phased Migration Planning**: For each repository pattern element, indicate whether it is:
   - "Critical" - Required for feature parity with the original procedure
   - "Enhancement" - Represents an architectural improvement over the original
   
   Then group components into suggested migration phases:
   - Phase 1: Core feature parity components
   - Phase 2: Performance optimization components
   - Phase 3: Architectural improvement components

8. **Use Code Analysis**: Make full use of the analysis.json file, which contains valuable metadata including:
   - Logical blocks and their relationships
   - Table references and operations
   - Potential business rules at a granular level
   - Data flow information
   - Implementation complexity assessments
   - Repository boundaries
   - Test value suggestions

## Response Format
Return both JSON files with complete, properly formatted content. Do not truncate or abbreviate. Structure each file according to the templates above, with meaningful, specific content derived from the SQL procedure and analysis.

Please analyze the following files:
{sql_code}
{meta_data}


        """,
        expected_output="""
        VALID JSON FOLLOWING THIS TEMPLATE: 

"technical": {
{
  "businessRules": [
    {
      "ruleId": "BR-001",
      "ruleName": "descriptive name",
      "action": "what happens when rule is triggered",
      "category": "validation/calculation/workflow/etc",
      "description": "detailed explanation",
      "trigger": "conditions that activate rule",
      "entities": ["business objects affected"],
      "implementation": {
        "sqlCode": "sql code from which the rule is generated",
        "technicalDescription": "how rule is implemented",
        "line_start": "line on which the code of the rule begins",
        "line_end": "line on which the code of the rule ends"
      },
      "confidence": 0-100 score,
      "reasoning": "explanation for this rule extraction",
      "testValues": {
        "normalCase": ["values that should pass/trigger normally"],
        "boundaryCase": ["values at the boundaries"],
        "edgeCase": ["exceptional values to test"]
      }
    }
  ],
  "businessFunctions": [
    {
      "functionId": "BF-001",
      "functionName": "descriptive name",
      "description": "business description of this capability",
      "componentRules": ["BR-001", "BR-002", "..."],
      "repositoryMethod": {
        "name": "methodName",
        "returnType": "return type",
        "parameters": [
          {
            "name": "parameterName",
            "type": "parameter type",
            "description": "parameter description"
          }
        ],
        "sqlImplementationNotes": [
          {
            "ruleId": "BR-001",
            "sqlSnippet": "relevant SQL code snippet"
          }
        ]
      },
      "dataRequirements": {
        "inputs": ["required input data"],
        "outputs": ["expected output data"],
        "dependencies": ["external systems or data needed"]
      }
    }
  ],
  "businessProcesses": [
    {
      "processId": "PROC-001",
      "processName": "descriptive name",
      "description": "business description",
      "orchestration": {
        "steps": [
          {
            "sequence": 1,
            "functionId": "BF-001",
            "description": "step description"
          }
        ]
      }
    }
  ],
“ImplementationApproach” : {
  "dataModels": [
    {
      "modelName": "Entity name",
      "description": "entity description",
      "properties": [
        {
          "name": "property name",
          "type": "data type",
          "description": "property description"
        }
      ],
      "relationships": [
        {
          "relatedModel": "related entity",
          "type": "relationship type",
          "description": "relationship description"
        }
      ]
    }
  ],
  "transactionBoundaries": {
    "startPoints": ["line reference: BEGIN TRANSACTION"],
    "endPoints": ["line reference: COMMIT/ROLLBACK"],
    "savePoints": ["any SAVE TRANSACTION points"],
    "repositoryImplications": "guidance for C# transaction handling",
    "parityRequirement": "Critical/Enhancement"
  },
  "dependencyInjectionRequirements": [
    {
      "dependency": "name of external service or component",
      "usage": "how and where it's used in the procedure",
      "suggestedInterface": "IServiceName for C# implementation",
      "parityRequirement": "Critical/Enhancement"
    }
  ],
  "stateManagement": {
    "temporaryStorageUsage": [
      {
        "structure": "table variable or temp table name",
        "purpose": "what it stores and why",
        "c#Alternative": "suggested C# data structure",
        "parityRequirement": "Critical/Enhancement"
      }
    ]
  },
  "errorHandlingPatterns": {
    "patterns": [
      {
        "approach": "how errors are handled in SQL",
        "c#Implication": "suggested C# error handling approach",
        "parityRequirement": "Critical/Enhancement"
      }
    ]
  },
  "dataVolumeConsiderations": {
    "largeDatasetOperations": [
      {
        "operation": "description of operation that might handle large data",
        "volumeImplication": "potential impact",
        "c#Approach": "suggested approach for handling in C#",
        "parityRequirement": "Critical/Enhancement"
      }
    ]
  },
  "repositoryPatternGuidance": {
    "queryOptimization": {
      "sqlPatterns": [
        {
          "pattern": "description of SQL query pattern",
          "location": "where in the code this appears",
          "c#Consideration": "guidance for implementing in C#",
          "performanceImpact": "level of performance impact",
          "parityRequirement": "Critical/Enhancement"
        }
      ]
    },
    "concurrencyHandling": {
      "sqlApproach": "how concurrency is handled in SQL",
      "c#Recommendation": "suggested C# approach",
      "implementationNotes": "details for implementation",
      "parityRequirement": "Critical/Enhancement"
    },
    "bulkOperations": [
      {
        "operation": "description of bulk operation",
        "sqlImplementation": "how it's implemented in SQL",
        "c#Alternative": "C# approach to implement",
        "performanceConsideration": "performance implications",
        "parityRequirement": "Critical/Enhancement"
      }
    ],
    "cachingOpportunities": [
      {
        "target": "what data could be cached",
        "pattern": "usage pattern in the code",
        "c#Approach": "suggested caching approach",
        "benefit": "expected benefit",
        "parityRequirement": "Critical/Enhancement"
      }
    ],
    "sqlServerFunctions": [
      {
        "function": "SQL Server function used",
        "usage": "how it's used in the code",
        "c#Equivalent": "C# equivalent function or method",
        "notes": "implementation notes",
        "parityRequirement": "Critical/Enhancement"
      }
    ],
    "databaseSpecificFeatures": [
      {
        "feature": "SQL Server specific feature",
        "usage": "how it's used",
        "c#Approach": "C# approach to implement",
        "migrationComplexity": "complexity level",
        "parityRequirement": "Critical/Enhancement"
      }
    ],
    "dataProjectionPatterns": [
      {
        "pattern": "how data is projected in SQL",
        "sqlImplementation": "SQL implementation details",
        "c#Recommendation": "C# recommendation",
        "benefit": "expected benefit",
        "parityRequirement": "Critical/Enhancement"
      }
    ],
    "dynamicSqlUsage": {
      "detected": true/false,
      "notes": "details if any",
      "parityRequirement": "Critical/Enhancement"
    },
    "crossCuttingConcerns": [
      {
        "concern": "cross-cutting concern identified",
        "sqlImplementation": "how it's implemented in SQL",
        "c#Approach": "suggested C# approach",
        "architecturalConsideration": "architectural notes",
        "parityRequirement": "Critical/Enhancement"
      }
    ],
    "repositoryInterfaceDesign": {
      "recommendedInterfaces": [
        {
          "name": "suggested interface name",
          "purpose": "purpose of the interface",
          "suggestedMethods": ["method signatures"],
          "designPrinciples": "design guidance"
        }
      ]
    }
  },
  "migrationPhasing": {
    "phases": [
      {
        "phase": 1,
        "focus": "Core Feature Parity",
        "repositoryComponents": ["critical components for phase 1"],
        "testFocus": "testing approach for this phase"
      },
      {
        "phase": 2,
        "focus": "Performance Optimization",
        "repositoryComponents": ["components for phase 2"],
        "testFocus": "testing approach for this phase"
      },
      {
        "phase": 3,
        "focus": "Architectural Improvements",
        "repositoryComponents": ["components for phase 3"],
        "testFocus": "testing approach for this phase"
      }
    ],
    "componentsByPhase": {
      "phase1Components": ["critical component references"],
      "phase2Components": ["phase 2 component references"],
      "phase3Components": ["phase 3 component references"]
    }
  },
  "domainModelMapping": {
    "aggregateRoots": [
      {
        "name": "suggested domain entity name",
        "keyEntities": ["related database entities"],
        "repositoryResponsibility": "suggested repository name"
      }
    ]
  },
  "repositoryMigrationStrategy": {
    "approachOptions": [
      {
        "strategy": "name of migration approach",
        "description": "details of the approach",
        "benefit": "why this approach might be beneficial"
      }
    ]
  }
}
},
"business": [
  {
  "processSummary": {
    "name": "Process name based on stored procedure purpose",
    "description": "Business-friendly description of the process purpose"
  },
  "businessProcesses": [
    {
      "processId": "PROC-001",
      "processName": "descriptive name",
      "description": "business description",
      "inputs": ["business inputs"],
      "outputs": ["business outputs"],
      "outcome": "business outcome",
      "steps": [
        {
          "sequence": 1,
          "name": "step name",
          "description": "step description",
          "responsibleRole": "[To be determined]",
          "decisions": ["decision points in this step"]
        }
      ]
    }
  ],
  "businessFunctions": [
    {
      "functionId": "BF-001", 
      "functionName": "descriptive name",
      "description": "business description",
      "responsibleRole": "[To be determined]",
      "inputs": ["business inputs"],
      "outputs": ["business outputs"],
      "relatedRules": ["BR-001", "BR-002"]
    }
  ],
  "businessRules": [
    {
      "ruleId": "BR-001",
      "ruleName": "descriptive name",
      "ruleStatement": "business language statement of rule",
      "rationale": "business reason for this rule",
      "enforcedBy": "[To be determined]",
      "impact": "[To be determined]",
      "examples": {
        "inclusion": ["examples of what the rule includes"],
        "exclusion": ["examples of what the rule excludes"]
      }
    }
  ],
  "businessEntities": [
    {
      "entityName": "entity name",
      "description": "entity description",
      "keyAttributes": ["important attributes"],
      "relationships": [
        {
          "relatedEntity": "related entity",
          "description": "relationship description"
        }
      ]
    }
  ],
  "businessDecisions": [
    {
      "decisionId": "DEC-001",
      "description": "decision description",
      "criteria": ["decision criteria"],
      "outcomes": ["possible outcomes"],
      "relatedRules": ["BR-001"]
    }
  ],
  "governanceGuidance": {
    "description": "This document provides a starting point for business rules governance. The following elements require business stakeholder input to complete the transition from implementation-focused to business-rule-focused governance:",
    "toBeDetermined": [
      "Process owners and stakeholders",
      "Rule enforcement responsibility",
      "Ongoing governance procedures"
    ]
  },
  "technicalContext": {
    "prioritizedObservations": [
      {
        "priority": "High/Medium/Low",
        "area": "Technical area of interest",
        "observation": "What was observed in the code",
        "businessImpact": "How this affects business operations or capabilities",
        "riskFactor": "Potential risks associated with this observation"
      }
    ],
    "migrationConsiderations": {
      "keyConsiderations": [
        {
          "area": "Technical area requiring special attention",
          "businessContext": "Why this matters from a business perspective",
          "migrationImplication": "How this affects the repository pattern implementation",
          "phasedApproach": "How this fits into the phased migration strategy"
        }
      ],
      "phasedMigrationSummary": {
        "phase1": {
          "focus": "Core functional parity",
          "businessBenefit": "Ensures continued business operations with same results",
          "timeframeGuidance": "Suggested implementation considerations"
        },
        "phase2": {
          "focus": "Performance optimization",
          "businessBenefit": "Improved system response and throughput",
          "timeframeGuidance": "Suggested implementation considerations"
        },
        "phase3": {
          "focus": "Architectural improvements",
          "businessBenefit": "Enhanced maintainability and adaptability",
          "timeframeGuidance": "Suggested implementation considerations"
        }
      }
    }
  }
}

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

    print(f"Business logic analysis completed for {procedure}")

    # Create analysis directory for the selected procedure
    analysis_dir = os.path.join("output/analysis", procedure)
    os.makedirs(analysis_dir, exist_ok=True)

    # Save the result to a JSON file
    with open(os.path.join(analysis_dir, f"{procedure}_business_logic.json"), "w") as f:
        f.write(result.replace("```json", "").replace("```", ""))

print("Business logic analysis completed for all procedures.")