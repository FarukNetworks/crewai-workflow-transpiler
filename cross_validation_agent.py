from crewai import Crew, Agent, Task, LLM
import os
import json
import boto3
import dotenv

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
    role="Software Architect",
    goal="SQL to C# Migration Behavioral Parity Verification",
    backstory="You are an experienced software architect with a strong understanding of the SQL and C# programming languages. You are tasked with verifying the behavioral parity between the SQL stored procedure and its C# implementation.",
    allow_code_execution=False,
    llm=llm_config,
)

# Create a coding agent
agentBedrock = Agent(
    role="Software Architect",
    goal="SQL to C# Migration Behavioral Parity Verification",
    backstory="You are an experienced software architect with a strong understanding of the SQL and C# programming languages. You are tasked with verifying the behavioral parity between the SQL stored procedure and its C# implementation.",
    allow_code_execution=False,
    llm=bedrock_config,
)

# Create a coding agent
agentOpenai = Agent(
    role="Software Architect",
    goal="SQL to C# Migration Behavioral Parity Verification",
    backstory="You are an experienced software architect with a strong understanding of the SQL and C# programming languages. You are tasked with verifying the behavioral parity between the SQL stored procedure and its C# implementation.",
    allow_code_execution=False,
    llm=openai_config,
)


# Create Crew For Each Discovered Stored Procedure
for procedure in procedures:
    # Read procedure definition from SQL file
    with open(f"output/sql_raw/{procedure}/{procedure}.sql", "r") as f:
        procedure_definition = f.read()

    dependency_folder = os.path.join("output/data", "procedure_dependencies.json")
    with open(dependency_folder, "r") as f:
        all_dependencies = json.load(f)

    # Find the procedure with matching name in the dependencies list
    dependencies = []
    for proc in all_dependencies:
        if proc.get("name") == procedure:
            dependencies = proc.get("dependencies", [])
            break

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

    # Get all C# code files from procedure (recursively)
    csharp_files = []
    procedure_dir = f"output/csharp-code/{procedure}"

    # Function to recursively collect all .cs files
    def collect_cs_files(directory, base_path):
        files_list = []
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            relative_path = os.path.join(base_path, item) if base_path else item

            if os.path.isdir(item_path):
                # Recursively process subdirectories
                files_list.extend(collect_cs_files(item_path, relative_path))
            elif item.endswith(".cs"):
                # Add C# file to the list
                with open(item_path, "r") as f:
                    file_content = f.read()
                    files_list.append(
                        {
                            "filename": item,
                            "path": f"{procedure}/{relative_path}",
                            "code": file_content,
                        }
                    )
        return files_list

    # Collect all C# files recursively
    csharp_files = collect_cs_files(procedure_dir, "")

    # Convert the list of files to a formatted string
    csharp_code = ""
    for file_info in csharp_files:
        csharp_code += f"FILE: {file_info['path']}\n{file_info['code']}\n\n"

    print(f"Found {len(csharp_files)} C# files for procedure {procedure}")

    def create_task(agent):
        # Create a task that requires code execution
        return Task(
            description=f"""
    CSHARP CODE: 
    {csharp_code}
    STORED PROCEDURE CODE: {procedure_definition}

    # SQL to C# Migration Behavioral Parity Verification

    ## OBJECTIVE
    Perform a rigorous behavioral equivalence analysis between the provided SQL stored procedure and its C# implementation. Your primary goal is to verify and document evidence that the C# implementation can directly replace the stored procedure with identical behavior and outputs. Focus exclusively on behavioral parity, not code improvements or modernization.

    ## CRITICAL CONSIDERATIONS
    Before proceeding with detailed analysis, examine these frequently overlooked areas:

    1. **Silent Validation Changes**: C# implementations often add input validation not present in SQL (e.g., rejecting negative IDs that SQL would process)
    2. **HTTP Status Codes**: Controllers may return 400/404 errors while the original SQL would return empty results
    3. **Null vs. Empty**: SQL might return NULL while C# returns empty collections or default values
    4. **Permissiveness**: SQL procedures are typically more permissive with input values than C# implementations
    5. **Type Handling**: Different type handling behaviors between SQL and C# (truncation, rounding, etc.)

    ⚠️ Remember: True parity means the C# should behave **exactly** like the SQL, including accepting "problematic" inputs if the SQL does.

    ## VERIFICATION APPROACH

    ### 1. INTERFACE EQUIVALENCE
    - **Parameter Matching**: Verify each SQL parameter has an exact C# equivalent (name, type, nullability, default values)
    - **Return Structure**: Confirm the C# output structure matches the SQL result set exactly (columns, types, order)
    - **Status Codes/Return Values**: Verify return codes or output parameters are handled identically

    ### 2. BEHAVIORAL EQUIVALENCE
    - **Query Logic**: Confirm all WHERE clauses, JOINs, and filtering conditions are exactly replicated
    - **Data Modifications**: Verify identical INSERT, UPDATE, DELETE operations
    - **Transaction Boundaries**: Confirm transaction scope and commit/rollback behavior matches
    - **Concurrency Handling**: Verify locking behavior and concurrent access patterns match
    - **Null Handling**: Confirm NULL values are processed identically in both implementations
    - **Error Conditions**: Verify error raising, catching, and propagation matches
    - **Edge Cases**: Confirm behavior with boundary values, empty sets, and unexpected inputs

    ### 3. DATABASE INTERACTION PATTERNS
    - **Temporary Objects**: Verify handling of temp tables, table variables, or CTEs
    - **Execution Flow**: Confirm procedural logic, conditionals, and iteration match
    - **Side Effects**: Verify identical database state changes

    ## EVIDENCE DOCUMENTATION

    ### A. PARAMETER MAPPING TABLE
    | SQL Parameter | SQL Type | C# Parameter | C# Type | Equivalent? |
    |--------------|----------|--------------|---------|-------------|
    | @Param1      | int      | param1       | int     | Yes/No      |
    | ...          | ...      | ...          | ...     | ...         |

    ### B. RESULT SET MAPPING TABLE
    | SQL Column | SQL Type | C# Property | C# Type | Equivalent? |
    |------------|----------|------------|---------|-------------|
    | Column1    | varchar  | Property1  | string  | Yes/No      |
    | ...        | ...      | ...        | ...     | ...         |

    ### C. BEHAVIORAL TEST CASES
    | Test Case | Input Values | SQL Behavior | C# Behavior | Equivalent? |
    |-----------|--------------|--------------|------------|-------------|
    | Standard  | [typical]    | [describe]   | [describe] | Yes/No      |
    | Negative Values | [-1, -100, etc.] | [describe] | [describe] | Yes/No |
    | Zero Values | [0, 0.0, etc.] | [describe] | [describe] | Yes/No |
    | Boundary | [min/max] | [describe] | [describe] | Yes/No |
    | Null | [nulls] | [describe] | [describe] | Yes/No |
    | Empty Strings | [""] | [describe] | [describe] | Yes/No |
    | Special Chars | [',",;,--] | [describe] | [describe] | Yes/No |
    | Error | [invalid] | [describe] | [describe] | Yes/No |

    **MUST TEST**: Values the SQL procedure accepts but might be rejected by C# validation (negative IDs, unusual characters, extreme values)

    ### D. EXECUTION FLOW COMPARISON
    Document how control flow (IF/ELSE, loops, early returns) are translated from SQL to C#. Provide side-by-side examples showing equivalent logic.

    ### E. TRANSACTION BEHAVIOR
    Document how transaction boundaries and isolation levels are maintained between implementations.

    ## PARITY ASSESSMENT

    ### VALIDATION DIFFERENCES SUMMARY
    Explicitly list any validation that exists in either implementation but not the other:
    - **SQL-only validation**: (List any validation present in SQL but missing in C#)
    - **C#-only validation**: (List any validation present in C# but missing in SQL)

    ### ERROR RESPONSE DIFFERENCES
    List differences in how errors are communicated:
    - **SQL procedure**: (How does SQL signal errors - return codes, result sets, exceptions?)
    - **C# implementation**: (How does C# signal errors - exceptions, empty lists, HTTP status codes?)

    ### OVERALL EQUIVALENCE RATING
    Select one:
    - **EXACT MATCH**: Behavior identical in all scenarios
    - **FUNCTIONAL MATCH**: Minor variations but functionally equivalent 
    - **PARTIAL MATCH**: Works for primary cases but edge cases differ
    - **MISMATCH**: Significant behavioral differences

    ### PARITY GAPS
    For each identified difference:
    1. Describe the specific difference
    2. Provide example inputs that demonstrate the difference
    3. Explain expected SQL behavior vs. actual C# behavior
    4. Assess severity (Critical, Major, Minor)

    ### VERIFICATION CONFIDENCE
    Rate confidence in verification (High, Medium, Low) with explanation of factors affecting confidence.

    ## SUMMARY

    ### EQUIVALENCE EVIDENCE
    Summarize key evidence proving the implementations are behaviorally equivalent. Focus on the most compelling proof points.

    ### CRITICAL VERIFICATION GAPS
    Identify scenarios or behaviors that could not be fully verified through analysis alone, requiring additional testing.

    ### REPLACEMENT READINESS ASSESSMENT
    Provide a clear statement on whether the C# implementation can directly replace the stored procedure with identical behavior, based on the evidence collected.



            """,
            expected_output="""
    Detailed report in markdown format. 
    """,
            agent=agent,
        )

    task = create_task(agent)
    taskBedrock = create_task(agentBedrock)
    taskOpenai = create_task(agentOpenai)

    # Create a crew and add the task
    crew = Crew(agents=[agent], tasks=[task])
    crewBedrock = Crew(agents=[agentBedrock], tasks=[taskBedrock])
    crewOpenai = Crew(agents=[agentOpenai], tasks=[taskOpenai])

    # Execute the crew
    result = str(crew.kickoff())
    resultBedrock = str(crewBedrock.kickoff())
    resultOpenai = str(crewOpenai.kickoff())

    print(f"Behavioral Parity Verification completed for {procedure}")

    # Create analysis directory for the selected procedure
    analysis_dir = os.path.join("output/analysis", procedure)
    os.makedirs(analysis_dir, exist_ok=True)

    # Save the result to a MARKDOWN file
    with open(
        os.path.join(analysis_dir, f"{procedure}_behavioral_parity_verification.md"),
        "w",
    ) as f:
        f.write(result)

    with open(
        os.path.join(
            analysis_dir, f"{procedure}_behavioral_parity_verification_bedrock.md"
        ),
        "w",
    ) as f:
        f.write(resultBedrock)

    with open(
        os.path.join(
            analysis_dir, f"{procedure}_behavioral_parity_verification_openai.md"
        ),
        "w",
    ) as f:
        f.write(resultOpenai)

print("Behavioral Parity Verification completed for all procedures.")
