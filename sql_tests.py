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


if os.getenv("LLM_CONFIG") == "bedrock":
    llm_config = bedrock_config
elif os.getenv("LLM_CONFIG") == "anthropic":
    llm_config = anthropic_config
else:
    llm_config = openai_config

# Create a coding agent
agent = Agent(
    role="SQL Developer",
    goal="Analyze the stored procedure and provide tSQLt code.",
    backstory="You are an experienced SQL developer with strong SQL skills analyzing stored procedures and understanding the business logic behind the code that will lead to creating the FULL coverage for tSQLt test code. You will always use Unique naming with time in seconds suffix for test cases, fake tables, procedures, functions.",
    allow_code_execution=False,
    llm=llm_config,
    verbose=True,
)



def fix_agent(batch, error):
    fix_agent = Agent(
        role="tSQLt Error Fixer",
        goal="Fix tSQLt syntax and execution errors in tSQLt test code.",
        backstory="You are an expert in diagnosing and fixing tSQLt errors in tests.",
        allow_code_execution=False,
        llm=llm_config,
        verbose=True,
    )

    task = Task(
        description=f"""
        This is the error: 
        Error: {error}

        Only fix the error in this code:
        {batch}

        NOTE: Use UNIQUE test names and #temp table names for each test case and make sure the mocking objects are also unique. You can use current time in seconds as a suffix to prevent name conflicts.
        NOTE: Please make sure that data is unique for each test case. Be sure the ID's and other values are not in used so it needs to be unique.
            

        """,
        expected_output="""
        SQL Code. Return fixed SQL code without any comments
        """,
        agent=fix_agent
    )

    crew = Crew(
        agents=[fix_agent],
        tasks=[task]
    )

    result = str(crew.kickoff())
    result = result.replace("```sql", "").replace("```", "")
    result = result.replace('GO', '')
    return result


# Create Crew For Each Discovered Stored Procedure 
for procedure in procedures:
    # Create test directory path
    test_dir = os.path.join("output", "sql-tests", procedure)
    test_file_path = os.path.join(test_dir, f"{procedure}_test.sql")
    
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
        with open(f"output/analysis/{procedure}/{procedure}_business_logic.json", "r") as f:
            business_logic = json.load(f)

        # Read integration test spec from JSON file
        with open(f"output/analysis/{procedure}/{procedure}_integration_test_spec.json", "r") as f:
            integration_test_spec = json.load(f)
        
        # Create a task that requires code execution
        task = Task(
            description=f"""
            Analyze the stored procedure `{procedure}` and provide tSQLt code

            The tests should cover:

            - Typical use cases (positive scenarios)
            - Edge cases (boundary conditions, invalid data, null handling)
            - Error conditions (failures, transactions, rollbacks)

            Structure tests clearly and provide scripts ready to execute.

            NOTE: Use UNIQUE test names and #temp table names for each test case and make sure the mocking objects are also unique. You can use current time in seconds as a suffix to prevent name conflicts.
            NOTE: Please make sure that data is unique for each test case. Be sure the ID's and other values are not in used so it needs to be unique.
            
            TEST SPECIFICATIOM: {integration_test_spec} 
            BUSINESS LOGIC: {business_logic}
            """,
            expected_output="""
            tSQLt code
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

        print(f"tSQLt code completed for {procedure}")
        result = result.replace("```sql", "").replace("```", "")

        # Strip Comments from Unit Test
        unit_test_code = sqlparse.format(result, strip_comments=True).strip()

        unit_test_code = re.sub(r'(EXEC\s+tSQLt\.NewTestClass\s+[^;]*;)', r'\1\nGO', unit_test_code, flags=re.IGNORECASE)

        # Create analysis directory for the selected procedure
        os.makedirs(test_dir, exist_ok=True)

        # Save the result to a SQL file
        with open(test_file_path, "w") as f:
            f.write(unit_test_code)
    
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
    
    batches = naive_linechunk(unit_test_code)

    # Upload Batches 
    attempt = 0 
    max_attempt = 5 
    for batch_index, batch in enumerate(batches):
        attempt = 0
        while attempt < max_attempt:
            try: 
                print("------THIS IS EXECUTION CODE------")
                print(batch)
                print("------THIS IS END EXECUTION CODE------")
                cursor.execute(batch)
                connection.commit()
                print(f"✅ Successfully uploaded batch for {procedure}")
                break  # Break out of retry loop if successful
            except Exception as e:  
                print(f"❌ Failed to upload batch for {procedure}: {e}")
                connection.rollback()
                attempt += 1
                if attempt < max_attempt:
                    print(f"🔄 Attempting fix and retry ({attempt}/{max_attempt})")
                    # Extract only the error message part
                    error_message = str(e)
                    if "[SQL Server]" in error_message:
                        error_message = error_message.split("[SQL Server]")[1].split("(")[0].strip()
                    fixed_batch = fix_agent(batch, error_message)
                    # Replace the old batch with the fixed one in the batches list
                    batches[batch_index] = fixed_batch
                    batch = fixed_batch
                    
                    # Save the updated batches back to the file
                    updated_unit_test_code = ""
                    for b in batches:
                        updated_unit_test_code += b + "\nGO\n"
                    
                    with open(test_file_path, "w") as f:
                        f.write(updated_unit_test_code)
                else:
                    print(f"❌ Failed to upload batch for {procedure} after {max_attempt} attempts")
        
    # Run the test 
    testClass = re.search(r'EXEC\s+tSQLt\.NewTestClass\s+\'([^\']+)\'', unit_test_code, re.IGNORECASE)
    testClass = testClass.group(1) if testClass else procedure
    print(f"🧪 Running test class: '{testClass}'")
    
    try:
        # Execute the test class
        cursor.execute(f"EXEC tSQLt.RunTestClass '{testClass}'")
        connection.commit()
        print(f"✅ Test execution completed for '{testClass}'")
        
        # Try to get column names first to understand the structure
        try:
            cursor.execute("SELECT * FROM tSQLt.TestResult")
            columns = [column[0] for column in cursor.description]
            print(f"📊 TestResult table columns: {columns}")
        except Exception as e:
            print(f"⚠️ Could not get TestResult table structure: {e}")
        
        # Try different column names for the class filter
        possible_class_columns = ['Class', 'TestClass', 'ClassName', 'TestClassName']
        results = None
        
        for col_name in possible_class_columns:
            try:
                print(f"🔍 Trying to query with column: {col_name}")
                cursor.execute(f"SELECT * FROM tSQLt.TestResult WHERE {col_name} = '{testClass}'")
                results = cursor.fetchall()
                if results:
                    print(f"✅ Found results using column name: {col_name}")
                    break
            except Exception as e:
                print(f"❌ Query with column '{col_name}' failed: {e}")
        
        # If all specific queries failed, try getting all results
        if not results:
            print("🔍 Fetching all test results...")
            cursor.execute("SELECT * FROM tSQLt.TestResult")
            results = cursor.fetchall()
        
        # Print the results in a more readable format
        if results:
            print(f"\n📝 Test Results for '{testClass}':")
            for i, row in enumerate(results):
                print(f"\nResult {i+1}:")
                for j, value in enumerate(row):
                    if cursor.description:
                        col_name = cursor.description[j][0]
                        print(f"  {col_name}: {value}")
                    else:
                        print(f"  Column {j}: {value}")
            
            # Save results to JSON file
            try:
                # Convert results to a list of dictionaries for JSON serialization
                json_results = []
                column_names = [column[0] for column in cursor.description]
                
                for row in results:
                    # Convert row to a dictionary with column names as keys
                    row_dict = {}
                    for i, value in enumerate(row):
                        # Handle non-serializable types
                        if isinstance(value, (bytes, bytearray)):
                            row_dict[column_names[i]] = value.decode('utf-8', errors='replace')
                        elif hasattr(value, 'isoformat'):  # For datetime objects
                            row_dict[column_names[i]] = value.isoformat()
                        else:
                            # Try to serialize, or convert to string if not serializable
                            try:
                                json.dumps(value)
                                row_dict[column_names[i]] = value
                            except (TypeError, OverflowError):
                                row_dict[column_names[i]] = str(value)
                    
                    json_results.append(row_dict)
                
                # Write the results to a JSON file
                with open(f"sql-tests/{procedure}/results/{procedure}_test_results.json", "w") as f:
                    json.dump(json_results, f, indent=4, default=str)
                
                print(f"✅ Test results saved to sql-tests/{procedure}/results/{procedure}_test_results.json")
            except Exception as e:
                print(f"❌ Error saving test results to JSON: {e}")
        else:
            print(f"⚠️ No test results found for '{testClass}'")
            
    except Exception as e:
        print(f"❌ Error during test execution or result fetching: {e}")


   # THIS IS THE FIXING EXECUTION PART
    for procedure in procedures:
        test_dir = os.path.join("sql-tests", procedure)
        test_file_path = os.path.join(test_dir, f"{procedure}_test.sql")
        results_dir = os.path.join(test_dir, "results")
        results_file_path = os.path.join(results_dir, f"{procedure}_test_results.json")
        
        # Check if test file and results file exist
        if not os.path.exists(test_file_path) or not os.path.exists(results_file_path):
            print(f"⚠️ Test file or results file not found for {procedure}, skipping fix")
            continue
        
        print(f"🔍 Checking test results for {procedure}...")
        
        # Read test file and results
        with open(test_file_path, "r") as f:
            unit_test_code = f.read()
        
        try:
            with open(results_file_path, "r") as f:
                json_results = json.load(f)
        except json.JSONDecodeError:
            print(f"⚠️ Invalid JSON in results file for {procedure}, skipping fix")
            continue
        
        # Extract test class name from the test file
        test_class_match = re.search(r'EXEC\s+tSQLt\.NewTestClass\s+\'([^\']+)\'', unit_test_code, re.IGNORECASE)
        if not test_class_match:
            print(f"⚠️ Could not find test class name in {procedure}_test.sql, skipping fix")
            continue
        
        test_class_name = test_class_match.group(1)
        print(f"📋 Found test class: {test_class_name}")
        
        # Track if any fixes were made
        fixes_made = False
        
        # Process each failed test
        for result in json_results:
            if result.get("Result") == "Error":
                test_case_name = result.get("TestCase")
                error_message = result.get("Msg", "Unknown error")
                
                print(f"🔧 Fixing test: {test_case_name}")
                print(f"❌ Error: {error_message}")
                
                # Find the test procedure in the SQL file
                test_proc_pattern = re.compile(
                    r'CREATE\s+PROCEDURE\s+' + re.escape(test_class_name) + 
                    r'\.\[' + re.escape(test_case_name) + r'\][\s\S]+?END;[\s\n]*GO', 
                    re.IGNORECASE
                )
                
                test_proc_match = test_proc_pattern.search(unit_test_code)
                if not test_proc_match:
                    print(f"⚠️ Could not find test procedure for {test_case_name}, skipping")
                    continue
                
                # Extract the test procedure batch
                test_batch = test_proc_match.group(0)
                
                # Use fix_agent to fix the batch
                print(f"🤖 Asking fix agent to repair test: {test_case_name}")
                fixed_batch = fix_agent(test_batch, error_message)
                
                if fixed_batch == test_batch:
                    print(f"⚠️ Fix agent did not make any changes to {test_case_name}")
                    continue
                
                # Replace the original batch with the fixed one
                unit_test_code = unit_test_code.replace(test_batch, fixed_batch)
                fixes_made = True
                
                print(f"✅ Fixed test: {test_case_name}")
        
        # Save the updated test file if fixes were made
        if fixes_made:
            with open(test_file_path, "w") as f:
                f.write(unit_test_code)
            print(f"💾 Saved fixed test file: {test_file_path}")
            
            # Re-run the tests
            print(f"🧪 Re-running tests for {test_class_name}")
            
            # Split into GO-separated batches
            batches = naive_linechunk(unit_test_code)
            
            # Execute each batch
            for batch_index, batch in enumerate(batches):
                attempt = 0
                max_attempt = 5
                
                while attempt < max_attempt:
                    try:
                        print("------THIS IS EXECUTION CODE------")
                        print(batch)
                        print("------THIS IS END EXECUTION CODE------")
                        cursor.execute(batch)
                        connection.commit()
                        print(f"✅ Successfully executed batch {batch_index+1}/{len(batches)} for {procedure}")
                        break  # Break out of retry loop if successful
                    except Exception as e:
                        print(f"❌ Failed to execute batch {batch_index+1}/{len(batches)} for {procedure}: {e}")
                        connection.rollback()
                        attempt += 1
                        
                        if attempt < max_attempt:
                            print(f"🔄 Attempting fix and retry ({attempt}/{max_attempt})")
                            # Extract only the error message part
                            error_message = str(e)
                            if "[SQL Server]" in error_message:
                                error_message = error_message.split("[SQL Server]")[1].split("(")[0].strip()
                            
                            fixed_batch = fix_agent(batch, error_message)
                            
                            # Replace the old batch with the fixed one in the batches list
                            batches[batch_index] = fixed_batch
                            batch = fixed_batch
                            
                            # Update the full test code
                            updated_unit_test_code = ""
                            for b in batches:
                                updated_unit_test_code += b + "\nGO\n"
                            
                            with open(test_file_path, "w") as f:
                                f.write(updated_unit_test_code)
                        else:
                            print(f"❌ Failed to execute batch {batch_index+1}/{len(batches)} for {procedure} after {max_attempt} attempts")
            
            # Run the test class
            try:
                print(f"🧪 Running test class: '{test_class_name}'")
                cursor.execute(f"EXEC tSQLt.RunTestClass '{test_class_name}'")
                connection.commit()
                print(f"✅ Test execution completed for '{test_class_name}'")
                
                # Fetch and display test results
                cursor.execute(f"SELECT * FROM tSQLt.TestResult WHERE Class = '{test_class_name}'")
                results = cursor.fetchall()
                
                if results:
                    print(f"\n📝 Updated Test Results for '{test_class_name}':")
                    for i, row in enumerate(results):
                        print(f"\nResult {i+1}:")
                        for j, value in enumerate(row):
                            if cursor.description:
                                col_name = cursor.description[j][0]
                                print(f"  {col_name}: {value}")
                    
                    # Save updated results to JSON file
                    try:
                        # Convert results to a list of dictionaries for JSON serialization
                        json_results = []
                        column_names = [column[0] for column in cursor.description]
                        
                        for row in results:
                            # Convert row to a dictionary with column names as keys
                            row_dict = {}
                            for i, value in enumerate(row):
                                # Handle non-serializable types
                                if isinstance(value, (bytes, bytearray)):
                                    row_dict[column_names[i]] = value.decode('utf-8', errors='replace')
                                elif hasattr(value, 'isoformat'):  # For datetime objects
                                    row_dict[column_names[i]] = value.isoformat()
                                else:
                                    # Try to serialize, or convert to string if not serializable
                                    try:
                                        json.dumps(value)
                                        row_dict[column_names[i]] = value
                                    except (TypeError, OverflowError):
                                        row_dict[column_names[i]] = str(value)
                            
                            json_results.append(row_dict)
                        
                        # Write the updated results to a JSON file
                        with open(results_file_path, "w") as f:
                            json.dump(json_results, f, indent=4, default=str)
                        
                        print(f"✅ Updated test results saved to {results_file_path}")
                    except Exception as e:
                        print(f"❌ Error saving updated test results to JSON: {e}")
                else:
                    print(f"⚠️ No test results found for '{test_class_name}' after fixes")
            except Exception as e:
                print(f"❌ Error during test execution or result fetching after fixes: {e}")
        else:
            print(f"ℹ️ No fixes needed for {procedure}")

# Close the database connection
connection.close()
print("✅ tSQLt code completed for all procedures.")
