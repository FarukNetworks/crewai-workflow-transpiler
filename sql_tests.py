from crewai import Crew, Agent, Task, LLM
import os
import json
import boto3
import dotenv
import sqlparse
import pyodbc
import re
from datetime import datetime

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
    role="tSQLt Developer",
    goal="Analyze the business rule and stored procedure then provide tSQLt code.",
    backstory="""You are an experienced SQL developer with strong SQL skills analyzing stored procedures and 
    understanding the business logic behind the code that will lead to creating 
    the FULL coverage for tSQLt test code. Always use unique naming for mock data and mock tables. 
    """,
    allow_code_execution=False,
    llm=llm_config,
    verbose=True,
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

        # Read business logic from JSON file
        with open(
            f"output/analysis/{procedure}/{procedure}_business_logic.json", "r"
        ) as f:
            business_logic = json.load(f)

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

        dependency_folder = os.path.join("output/data", "procedure_dependencies.json")
        with open(dependency_folder, "r") as f:
            all_dependencies = json.load(f)

        # Find the procedure with matching name in the dependencies list
        dependencies = []
        for proc in all_dependencies:
            if proc.get("name") == procedure:
                dependencies = proc.get("dependencies", [])
                break

        # Parse Integration Test Specifications
        try:
            testScenarios = integration_test_spec["integrationTestSpecification"][
                "testScenarios"
            ]
        except KeyError:
            print(f"⚠️ No testScenarios found in integration test spec for {procedure}")
            testScenarios = []

        for scenario in testScenarios:
            scenarioId = scenario["scenarioId"]
            name = scenario["name"]
            businessProcesses = scenario["businessProcesses"]
            description = scenario["description"]
            inputs = scenario["inputs"]
            execution = scenario["execution"]
            verification = scenario["verification"]
            variations = scenario["variations"]

            print(scenario)

            # Create a task that requires code execution
            task = Task(
                description=f"""
                NOTE: EXEC tSQLt.NewTestClass test_{procedure} is already created, so don't include it in your code but follow the naming. 
                You are tasked to create test scenario based on this {procedure} stored procedure. 

                STORED PROCEDURE CODE:
                {procedure_code}   

                PROCEDURE META: 
                {meta_data}

                This is test scenario {scenarioId} named {name}. 
                
                BUSINESS PROCESSES:
                {businessProcesses}
                
                DESCRIPTION:
                {description}
                
                INPUTS:
                {inputs}
                
                EXECUTION:
                {execution}
                
                VERIFICATION:
                {verification}
                
                DEPENDENCIES:
                {dependencies}
                
                VARIATIONS:
                {variations}

                
                """,
                expected_output="""
                tSQLt code without any comments. Please do not create any new class it's already provided. EXEC tSQLt.NewTestClass test_{procedure}. 
                Begin the code like this: 
                CREATE PROCEDURE [test_{procedure}].[test_{procedure}_{scenarioId}].
                After Each procedure add GO 
                create and print before and after to have snapshots. 
                Use SELECT * INTO #BeforeTableName and SELECT * INTO #AfterTableName
                Then print the before and after tables like this: 
                SELECT *, '#BeforeTableName' AS table_name FROM #BeforeTableName 
                SELECT *, '#AfterTableName' AS table_name FROM #AfterTableName
                """,
                agent=agent,
            )

            # Create a crew and add the task
            crew = Crew(agents=[agent], tasks=[task], verbose=True)

            # Execute the crew
            result = str(crew.kickoff())

            print(result)

            print(f"tSQLt code completed for {procedure}")
            result = result.replace("```sql", "").replace("```", "")

            # Strip Comments from Unit Test
            unit_test_code = sqlparse.format(result, strip_comments=True).strip()

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
                    f.write(f"\n\n--  Test scenario: {scenarioId} - {name}\n")
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
            cursor.execute(batch)
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
            upload_data = {
                "index": index,
                "batch": batch,
                "test_results": "Failed",
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
