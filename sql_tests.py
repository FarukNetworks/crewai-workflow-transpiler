from crewai import Crew, Agent, Task, LLM
from shared.get_dependencies import get_dependencies
import os
import json
import boto3
import dotenv
import sqlparse
import pyodbc
import re
from datetime import datetime
from crewai.knowledge.source.text_file_knowledge_source import TextFileKnowledgeSource

dotenv.load_dotenv()

connection_string = os.getenv("CONNECTION_STRING")

connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

# Get all folder names from analysis directory
procedures = []
if os.path.exists("output/analysis"):
    procedures = [
        folder
        for folder in os.listdir("output/analysis")
        if os.path.isdir(os.path.join("output/analysis", folder))
    ]
print(f"Discovered procedures: {procedures}")

# Create a text file knowledge source
text_source = TextFileKnowledgeSource(file_paths=["tsqlt.txt"])


openai_config = LLM(
    model="gpt-4o",
    api_key=os.getenv("OPENAI_API_KEY"),
    temperature=0.1,  # Ensures deterministic SQL generation
    max_retries=3,
    request_timeout=60,
    verbose=True,
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
    api_key=os.getenv("ANTHROPIC_API_KEY"),
    timeout=900,  # Handles large JSON specs
    request_timeout=900,
    max_retries=2,
    max_tokens=64000,  # Allows long test specs
    verbose=True,
)

if os.getenv("LLM_CONFIG") == "bedrock":
    llm_config = bedrock_config
elif os.getenv("LLM_CONFIG") == "anthropic":
    llm_config = anthropic_config
else:
    llm_config = openai_config


# Create a coding agent
agent = Agent(
    role="tSQLt Developer",
    goal="Analyze the business rule and stored procedure then provide tSQLt code.",
    backstory="""You are an experienced SQL developer with strong SQL skills analyzing stored procedures and 
    understanding the business logic behind the code that will lead to creating 
    the FULL coverage for tSQLt test code. Always use unique naming for mock data and mock tables.
    Version of tSQLt: Version:1.0.8083.3529 InstalledOnSQLServer: 15.00
    """,
    allow_code_execution=False,
    llm=llm_config,
    verbose=True,
    # knowledge_sources=[text_source],
)

# Create Crew For Each Discovered Stored Procedure
for procedure in procedures:
    # Create test directory path
    test_dir = os.path.join("output", "sql-tests", procedure)
    test_file_path = os.path.join(test_dir, f"{procedure}_test.sql")

    procedure_code_dir = os.path.join("output", "sql_raw", procedure)
    with open(os.path.join(procedure_code_dir, f"{procedure}.sql"), "r") as f:
        procedure_code = f.read()

    # Check if test file already exists
    if os.path.exists(test_file_path):
        print(f"✅ Test file already exists for {procedure}, skipping generation")
        # Read the existing test file
        with open(test_file_path, "r") as f:
            unit_test_code = f.read()
    else:
        print(f"🔄 Generating new test for {procedure}")
        # Read meta data from JSON file
        with open(f"output/analysis/{procedure}/{procedure}_meta.json", "r") as f:
            meta_data = json.load(f)

        # business_rules
        with open(
            f"output/analysis/{procedure}/{procedure}_business_rules.json", "r"
        ) as f:
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

        # Read integration test spec from JSON file
        with open(
            f"output/analysis/{procedure}/{procedure}_integration_test_spec.json", "r"
        ) as f:
            integration_test_spec = json.load(f)

        # ADD test class for each procedure
        test_class_code = f"""
EXEC tSQLt.NewTestClass 'test_{procedure}';
GO
        """

        # Create test directory for the selected procedure
        os.makedirs(test_dir, exist_ok=True)

        # Create file with test class code
        with open(test_file_path, "w") as f:
            f.write(test_class_code)
        print(f"✅ Test class created for {procedure}")

        dependencies = get_dependencies(procedure)

        # Parse Integration Test Specifications
        try:
            testScenarios = integration_test_spec["testScenarios"]
        except KeyError:
            print(f"⚠️ No testScenarios found in integration test spec for {procedure}")
            testScenarios = []

        for scenario in testScenarios:
            scenarioId = scenario["testId"]
            description = scenario["description"]

            print(scenario)

            # Create a task that requires code execution
            task = Task(
                description=f"""
I need you to convert a JSON test specification into an executable tSQLt test case for validating a stored procedure. I'll provide:

1. Procedure name 
{procedure}

2. A JSON test specification
THE JSON TEST SPECIFICATION STRUCTURE:
{scenario}

3. Stored procedure dependencies
{dependencies}

4. Stored procedure code 
{procedure_code}

YOUR TASK:
Create a complete, executable tSQLt test procedure that:
1. Follows tSQLt best practices
2. Accurately implements all aspects of the test specification
3. Captures key results for later comparison with C# tests
4. Handles proper test isolation using tSQLt.FakeTable
5. Implements all specified test data setup, validation, and cleanup


### 🚨 **DATA TYPE HANDLING GUIDELINES** 🚨
✅ **Ensure correct data type handling in the generated SQL**
- Convert all **GUIDs** (`UNIQUEIDENTIFIER`) properly using `CONVERT(UNIQUEIDENTIFIER, 'GUID_STRING')`
- Convert all **numeric values** (`BIGINT`, `INT`) properly using `CAST(value AS INT)` or `CAST(value AS BIGINT)`
- Ensure that **all string-to-number comparisons** explicitly cast string values to `BIGINT` or `INT`
- Handle **NULL values explicitly** using `ISNULL(column, default_value)`
- When inserting GUIDs into `TestDataResults`, use `ISNULL(GUID, '11111111-1111-1111-1111-111111111111')`


STANDARD TEST STRUCTURE:
Your test procedure MUST include these sections in order:
1. Test procedure declaration with name derived from testId
2. Test variable declaration section (including TestRunId)
3. Metadata capture initialization
4. Test environment setup (FakeTable, data setup)
5. Stored procedure execution
6. Result validation using tSQLt.Assert methods
7. Result data capture for C# comparison
8. Metadata status update
9. Cleanup section (if specified)

RESULT CAPTURE FORMAT:
Each test will capture results in permanent tables that must be created as part of the tSQLt setup script:
The SETUP is already created in the database with the following code:
CREATE TABLE UnitTest.TestMetadataResults (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    TestRunId UNIQUEIDENTIFIER NOT NULL,
    TestId NVARCHAR(50) NOT NULL,
    TestCategory NVARCHAR(50) NOT NULL,
    RuleFunction NVARCHAR(50) NOT NULL,
    ExecutionDateTime DATETIME NOT NULL DEFAULT GETDATE(),
    ExecutionStatus NVARCHAR(20) NOT NULL
);

CREATE TABLE UnitTest.TestDataResults (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    TestRunId UNIQUEIDENTIFIER NOT NULL,
    TestId NVARCHAR(50) NOT NULL,
    EntityName NVARCHAR(100) NOT NULL,
    EntityKey NVARCHAR(100) NOT NULL,
    PropertyName NVARCHAR(100) NOT NULL,
    PropertyValue NVARCHAR(MAX) NULL,
    PropertyType NVARCHAR(50) NOT NULL
);

You job is to update the database tables with the results of the test. 

In each test, you MUST:
1. Generate a unique TestRunId at the beginning:
   ```sql
   DECLARE @TestRunId UNIQUEIDENTIFIER = NEWID();
   ```

2. Initialize metadata with status 'Running':
   ```sql
   INSERT INTO TestResults.TestMetadataResults (TestRunId, TestId, TestCategory, RuleFunction, ExecutionDateTime, ExecutionStatus)
   VALUES (@TestRunId, 'TestIdFromJson', 'CategoryFromJson', 'RuleFunctionFromJson', GETDATE(), 'Running');
   ```

3. After validation, capture entity data:
   ```sql
   INSERT INTO TestResults.TestDataResults (TestRunId, TestId, EntityName, EntityKey, PropertyName, PropertyValue, PropertyType)
   VALUES (@TestRunId, 'TestIdFromJson', 'EntityName', 'EntityKeyValue', 'PropertyName', 'ActualValue', 'DataType');
   ```

4. Update metadata status at the end:
   ```sql
   UPDATE TestResults.TestMetadataResults
   SET ExecutionStatus = 'Passed'
   WHERE TestRunId = @TestRunId;
   ```

5. Each test should display the these tables with: 
SELECT * FROM UnitTest.TestMetadataResults
SELECT * FROM UnitTest.TestDataResults

6. Please write tSLt unit tests compatible with version 1.0. Avoid using newer assertions like AssertExists and stick to the core assertions available in v1.0 such as AssertEquals, AssertEqualsTable, ExpectException, AssertLike, and AssertNotEquals."
For example, instead of:
```sql
EXEC tSLt.AssertExists @ObjectName = 'dbo.MyProcedure', @Message = 'Procedure should exist';
```
Use approaches like:
```sql
-- Check if object exists using ExpectException
BEGIN TRY
  EXEC ('SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = ''dbo'' AND ROUTINE_NAME = ''MyProcedure''');
  -- If we get here, object exists
  EXEC tSLt.AssertEquals 1, 1, 'Object exists as expected';
END TRY
BEGIN CATCH
  EXEC tSLt.Fail 'Expected object dbo.MyProcedure does not exist';
END CATCH
```

Version of tSQLt: Version:1.0.8083.3529 InstalledOnSQLServer: 15.00
Specifically:
- Use only tSQLt features compatible with SQL Server 2016
- Avoid STRING_SPLIT() function (use a custom split function instead)
- Avoid JSON functions (parse JSON programmatically if needed)
- Use ISJSON() only if absolutely necessary
- Avoid STRING_AGG() function

IMPLEMENTATION GUIDELINES:
- Use EXEC tSQLt.FakeTable for all tables referenced in testDataSetup
- Implement 'CREATE' actions by populating fake tables with INSERT statements
- Mock any functions specified in systemConfiguration with tSQLt.FakeFunction
- Handle expected exceptions properly using tSQLt.ExpectException when specified
- For validation criteria, use appropriate tSQLt.Assert methods:
  * "exists" → tSQLt.AssertExists
  * "notExists" → tSQLt.AssertNotExists
  * "equals" → tSQLt.AssertEquals
- Use TRY/CATCH to handle potential errors and update metadata
- Add clear comments explaining the test logic and key sections
- Ensure fake table column types match the actual database schema


                """,
                expected_output=f"""
                Please do not create any new class it's already provided. EXEC tSQLt.NewTestClass test_{procedure}. 
                -- Begin the code like this: 
                       CREATE PROCEDURE [test_{procedure}].[test_{procedure}_{scenarioId}].
                -- Clearly specify the schema of every object to prevent confusion.
                -- When calling tSQLt.FakeTable, never use the @SchemaName parameter (deprecated).
                -- Use unique, meaningful mock data identifiers clearly linked to test scenarios
                -- Add "GO" after each CREATE PROCEDURE "END" statement.
                -- Verify output by counting inserted rows or verifying specific column values
                -- Always create snapshots of tables before and after executing procedure.
                -- Print snapshot comparisons clearly
                -- Do not write any execution code in the output and focus only on creating the test. 
                """,
                agent=agent,
            )

            # FIX TASK
            def fix_agent(batch, error):
                # Write error to JSON
                with open(
                    f"output/sql-tests/{procedure}/results/error_group.json", "w"
                ) as f:
                    json.dump({"error": error}, f, indent=4)
                print(f"📄 Error information saved for {test_name}")

                print("----------⛔️⛔️⛔️⛔️⛔️-------------")
                print(f"🔄 Fixing tSQLt code for {procedure}")
                print("----------⛔️⛔️⛔️⛔️⛔️-------------")
                fix_task = Task(
                    description=f"""
YOU ARE TASKED TO FIX THIS ERROR:
{error}
                    
Please fix the tSQLt code for the following test scenario:
{batch}
                    
DATA: 
1. Procedure name 
{procedure}

2. A JSON test specification
THE JSON TEST SPECIFICATION STRUCTURE:
{scenario}

3. Stored procedure dependencies
{dependencies}

4. Stored procedure code 
{procedure_code}
                    """,
                    expected_output=f"""
                    Fixed tSQLt code for the following test scenario:
                    {batch}
                    """,
                    agent=agent,
                )

                crew = Crew(
                    agents=[agent],
                    tasks=[fix_task],
                    verbose=True,
                    planning=True,
                    # knowledge_sources=[text_source],
                )
                # Execute the crew
                result = str(crew.kickoff())
                return result

            # Create a crew and add the task
            crew = Crew(
                agents=[agent],
                tasks=[task],
                verbose=True,
                planning=True,
                # knowledge_sources=[text_source],
            )

            # Execute the crew
            result = str(crew.kickoff())

            print(result)

            print(f"tSQLt code completed for {procedure}")
            result = result.replace("```sql", "").replace("```", "")

            # Strip Comments from Unit Test

            unit_test_code = result

            # unit_test_code = re.sub(r'(EXEC\s+tSQLt\.NewTestClass\s+[^;]*;)', r'\1\nGO', unit_test_code, flags=re.IGNORECASE)

            # Create analysis directory for the selected procedure
            os.makedirs(test_dir, exist_ok=True)

            # Save the result to a SQL file, but don't overwrite existing tests
            if not os.path.exists(test_file_path):
                with open(test_file_path, "w") as f:
                    f.write(unit_test_code)
                print(f"✅ New test file created for {procedure}")
            else:
                # Append to existing file with a separator
                with open(test_file_path, "a") as f:
                    f.write(f"\n\n--  Test scenario: {scenarioId} - {description}\n")
                    f.write(unit_test_code)
                print(f"✅ Test scenario appended to existing file for {procedure}")

    with open(test_file_path, "r") as f:
        test_file_code = f.read()

    # Split into GO-separated batches properly
    def naive_linechunk(sql_script):
        batches = []
        current_batch = []

        for line in sql_script.splitlines():
            if line.strip().upper() == "GO":
                batches.append("\n".join(current_batch))
                current_batch = []
            else:
                current_batch.append(line)

        if current_batch:
            batches.append("\n".join(current_batch))

        return [batch.strip() for batch in batches if batch.strip()]

    batches = naive_linechunk(test_file_code)
    os.makedirs(f"output/sql-tests/{procedure}/results", exist_ok=True)

    index = 0

    for batch in batches:
        try:
            cursor.execute(sqlparse.format(batch, strip_comments=True).strip())
            connection.commit()
            upload_data = {
                "index": index,
                "batch": batch,
                "test_results": "Uploaded",
            }
            with open(
                f"output/sql-tests/{procedure}/results/batch_{index}_upload_results.json",
                "w",
            ) as f:
                json.dump(upload_data, f, indent=4)
            print(f"✅ Successfully uploaded batch {index} for {procedure}")

            # Increment index here, inside the loop
            index += 1

            # Run the test
            # Extract test name from the batch by finding the CREATE PROCEDURE statement
            try:
                test_name = None
                for line in batch.splitlines():
                    if "CREATE PROCEDURE" in line:
                        # Extract the full procedure name with brackets
                        match = re.search(r"CREATE PROCEDURE (\[.*?\]\.\[.*?\])", line)
                        if match:
                            test_name = match.group(1)
                            break
            except Exception as e:
                print(f"❌ Failed to extract test name from batch for {procedure}: {e}")
                continue

            if test_name:
                # Run the test
                print(f"🧪 Running test: {test_name}")
                try:
                    cursor.execute(f"EXEC tSQLt.Run '{test_name}'")

                    test_results = []
                    messages = []

                    while True:
                        if cursor.description:
                            # Fetch query results and structure into JSON
                            columns = [column[0] for column in cursor.description]
                            rows = cursor.fetchall()
                            result_data = [dict(zip(columns, row)) for row in rows]

                            # Convert datetime objects to strings
                            for row in result_data:
                                for key, value in row.items():
                                    if isinstance(value, datetime):
                                        row[key] = value.isoformat()

                            if result_data:
                                test_results.append(result_data)

                        # Capture messages from SQL Server
                        for message in cursor.messages:
                            messages.append(message[1])

                        if not cursor.nextset():
                            break  # No more result sets

                    # Fetch tSQLt.TestResult table data
                    cursor.execute("SELECT * FROM [tSQLt].[TestResult]")
                    test_result_rows = cursor.fetchall()
                    columns = [column[0] for column in cursor.description]
                    test_result_data = [
                        dict(zip(columns, row)) for row in test_result_rows
                    ]

                    # Convert datetime objects in tSQLt.TestResult to strings
                    for row in test_result_data:
                        for key, value in row.items():
                            if isinstance(value, datetime):
                                row[key] = value.isoformat()

                    # Print execution messages
                    if messages:
                        print("\n📝 Execution Messages:")
                        for msg in messages:
                            print(msg)

                    # Print and save test results
                    if test_results:
                        print("✅ Structured Test Results:")
                        print(json.dumps(test_results, indent=4, default=str))

                        os.makedirs(
                            f"output/sql-tests/{procedure}/results", exist_ok=True
                        )

                        with open(
                            f"output/sql-tests/{procedure}/results/{test_name.replace('.', '_')}_test_results.json",
                            "w",
                        ) as f:
                            json.dump(test_results, f, indent=4, default=str)
                        print(f"📄 Test results saved for {test_name}")

                    # Save tSQLt.TestResult separately
                    if test_result_data:
                        with open(
                            f"output/sql-tests/{procedure}/results/{test_name.replace('.', '_')}_tsqlt_results.json",
                            "w",
                        ) as f:
                            json.dump(test_result_data, f, indent=4, default=str)
                        print(f"📄 tSQLt.TestResult saved for {test_name}")

                    # Save execution messages
                    if messages:
                        with open(
                            f"output/sql-tests/{procedure}/results/{test_name.replace('.', '_')}_messages.log",
                            "w",
                        ) as f:
                            f.write("\n".join(messages))
                        print(f"📄 Execution messages saved for {test_name}")

                    else:
                        print("⚠️ No test results returned from the database.")
                except Exception as e:
                    print(f"❌ Error running test {test_name}: {str(e)}")

                    # Even if the test fails, try to get the tSQLt.TestResult data
                    try:
                        cursor.execute("SELECT * FROM [tSQLt].[TestResult]")
                        test_result_rows = cursor.fetchall()
                        columns = [column[0] for column in cursor.description]
                        test_result_data = [
                            dict(zip(columns, row)) for row in test_result_rows
                        ]

                        # Convert datetime objects in tSQLt.TestResult to strings
                        for row in test_result_data:
                            for key, value in row.items():
                                if isinstance(value, datetime):
                                    row[key] = value.isoformat()

                        # Save tSQLt.TestResult for failed test
                        with open(
                            f"output/sql-tests/{procedure}/results/{test_name.replace('.', '_')}_tsqlt_results.json",
                            "w",
                        ) as f:
                            json.dump(test_result_data, f, indent=4, default=str)
                        print(f"📄 tSQLt.TestResult saved for failed test {test_name}")

                        # Save error information
                        error_data = {"error": str(e)}
                        with open(
                            f"output/sql-tests/{procedure}/results/{test_name.replace('.', '_')}_error.json",
                            "w",
                        ) as f:
                            json.dump(error_data, f, indent=4)
                        print(f"📄 Error information saved for {test_name}")
                    except Exception as inner_e:
                        print(
                            f"❌ Could not retrieve tSQLt results after test failure: {str(inner_e)}"
                        )
            else:
                print(f"⚠️ Could not extract test name from batch")
        except Exception as e:
            # FIX TASK with retry mechanism
            max_attempts = 3
            current_attempt = 1
            fixed_batch = batch
            error_message = str(e)

            while current_attempt <= max_attempts:
                print(
                    f"🔄 Fix attempt {current_attempt}/{max_attempts} for {procedure}"
                )
                try:
                    fixed_batch = fix_agent(fixed_batch, error_message)

                    # Try to execute the fixed batch to verify it works
                    cursor.execute(
                        sqlparse.format(fixed_batch, strip_comments=True).strip()
                    )
                    connection.commit()

                    # If we get here, the fix was successful
                    print(f"✅ Successfully fixed batch on attempt {current_attempt}")

                    # Update the test file with the fixed batch
                    with open(test_file_path, "r") as f:
                        test_file_content = f.read()

                    updated_content = test_file_content.replace(batch, fixed_batch)

                    with open(test_file_path, "w") as f:
                        f.write(updated_content)

                    print(f"✅ Updated test file for {procedure} with fixed batch.")

                    # Update the original batch reference for future iterations
                    batch = fixed_batch

                    # Break out of the retry loop since we succeeded
                    break

                except Exception as retry_error:
                    # The fix didn't work, try again
                    error_message = str(retry_error)
                    print(f"❌ Fix attempt {current_attempt} failed: {error_message}")
                    current_attempt += 1

            # Record the results of our fix attempts
            if current_attempt <= max_attempts:
                fix_status = "Fixed"
            else:
                fix_status = "Failed after max attempts"

            upload_data = {
                "index": index,
                "batch": batch,
                "test_results": "Failed",
                "fix_attempts": current_attempt,
                "fix_status": fix_status,
                "last_error": error_message,
            }
            with open(
                f"output/sql-tests/{procedure}/results/batch_{index}_upload_results.json",
                "w",
            ) as f:
                json.dump(upload_data, f, indent=4)
            print(f"❌ Failed to upload batch for {procedure}: {str(e)}")

    # After processing all batches, run all tests for this procedure
    try:
        print(f"🧪 Running all tests for {procedure}")
        cursor.execute(f"EXEC tSQLt.Run 'test_{procedure}'")

        test_results = []
        messages = []

        while True:
            if cursor.description:
                # Fetch query results and structure into JSON
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                result_data = [dict(zip(columns, row)) for row in rows]

                # Convert datetime objects to strings
                for row in result_data:
                    for key, value in row.items():
                        if isinstance(value, datetime):
                            row[key] = value.isoformat()

                if result_data:
                    test_results.append(result_data)

            # Capture messages from SQL Server
            for message in cursor.messages:
                messages.append(message[1])

            if not cursor.nextset():
                break  # No more result sets

        # Fetch tSQLt.TestResult table data for all tests in this class
        cursor.execute(
            f"SELECT * FROM [tSQLt].[TestResult] WHERE Class = 'test_{procedure}'"
        )
        test_result_rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        test_result_data = [dict(zip(columns, row)) for row in test_result_rows]

        # Convert datetime objects in tSQLt.TestResult to strings
        for row in test_result_data:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()

        # Save all test results
        with open(
            f"output/sql-tests/{procedure}/results/all_tests_results.json",
            "w",
        ) as f:
            json.dump(test_result_data, f, indent=4, default=str)
        print(f"📄 All test results saved for {procedure}")

        # Save execution messages
        if messages:
            with open(
                f"output/sql-tests/{procedure}/results/all_tests_messages.log",
                "w",
            ) as f:
                f.write("\n".join(messages))
            print(f"📄 All execution messages saved for {procedure}")

    except Exception as e:
        print(f"❌ Failed to run all tests for {procedure}: {str(e)}")

        # Even if running all tests fails, try to get the tSQLt.TestResult data
        try:
            cursor.execute(
                f"SELECT * FROM [tSQLt].[TestResult] WHERE Class = 'test_{procedure}'"
            )
            test_result_rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            test_result_data = [dict(zip(columns, row)) for row in test_result_rows]

            # Convert datetime objects in tSQLt.TestResult to strings
            for row in test_result_data:
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.isoformat()

            # Save all test results even if there was an error
            with open(
                f"output/sql-tests/{procedure}/results/all_tests_results.json",
                "w",
            ) as f:
                json.dump(test_result_data, f, indent=4, default=str)
            print(f"📄 All test results saved for {procedure} despite errors")

            # Save error information
            error_data = {"error": str(e)}
            with open(
                f"output/sql-tests/{procedure}/results/all_tests_error.json",
                "w",
            ) as f:
                json.dump(error_data, f, indent=4)
            print(f"📄 Error information saved for all tests")
        except Exception as inner_e:
            print(
                f"❌ Could not retrieve tSQLt results after all tests failure: {str(inner_e)}"
            )

# Close the database connection
connection.close()
print("✅ tSQLt code completed for all procedures.")
