import os
import json
import dotenv
import sqlparse
import pyodbc
import datetime

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

for procedure in procedures:
    with open(f"output/analysis/{procedure}/{procedure}_business_logic.json", "r") as f:
        business_logic = json.load(f)

    with open(
        f"output/analysis/{procedure}/{procedure}_integration_test_spec.json", "r"
    ) as f:
        integration_test_spec = json.load(f)

        # Parse Integration Test Specifications
        try:
            testScenarios = integration_test_spec["integrationTestSpecification"][
                "testScenarios"
            ]
        except KeyError:
            print(f"⚠️ No testScenarios found in integration test spec for {procedure}")
            testScenarios = []

        try:
            businessProcessesJson = business_logic["technical"]["businessProcesses"]
        except KeyError:
            print(f"⚠️ No businessProcesses found in business logic for {procedure}")
            businessProcessesJson = []

        discoveredBusinessProcesses = []

        for scenario in testScenarios:
            scenarioId = scenario["scenarioId"]
            name = scenario["name"]
            businessProcessesInsideScenario = scenario["businessProcesses"]
            description = scenario["description"]
            inputs = scenario["inputs"]
            execution = scenario["execution"]
            verification = scenario["verification"]
            variations = scenario["variations"]

            for businessProc in businessProcessesInsideScenario:
                for businessProcess in businessProcessesJson:
                    if businessProcess["processId"] == businessProc:
                        businessProcessName = businessProcess["processName"]
                        businessProcessDescription = businessProcess["description"]

                        if businessProcessName not in [
                            bp["processName"] for bp in discoveredBusinessProcesses
                        ]:
                            discoveredBusinessProcesses.append(
                                {
                                    "processId": businessProc,
                                    "processName": businessProcessName,
                                    "processDescription": businessProcessDescription,
                                    "testScenarios": [
                                        {
                                            "scenarioId": scenarioId,
                                            "scenarioName": name,
                                            "scenarioDescription": description,
                                        }
                                    ],
                                }
                            )
                        else:
                            # Add this scenario to existing business process
                            for bp in discoveredBusinessProcesses:
                                if bp["processId"] == businessProc:
                                    bp["testScenarios"].append(
                                        {
                                            "scenarioId": scenarioId,
                                            "scenarioName": name,
                                            "scenarioDescription": description,
                                        }
                                    )
                                    break
                        break

        # Enhance the test scenarios with SQL test code and results
        for bp in discoveredBusinessProcesses:
            for scenario in bp["testScenarios"]:
                scenario_id = scenario["scenarioId"]

                # Look for SQL test file
                sql_test_path = f"sql-tests/{procedure}/{procedure}_test.sql"
                if os.path.exists(sql_test_path):
                    with open(sql_test_path, "r") as sql_file:
                        sql_content = sql_file.read()

                        # Extract the specific test for this scenario
                        # Assuming tests are separated by GO statements or similar markers
                        # and contain the scenario ID in their name
                        sql_lines = sql_content.split("\n")
                        scenario_test_code = []
                        capture = False

                        for line in sql_lines:
                            if (
                                f"SCEN-{scenario_id.split('-')[1]}" in line
                                or f"{scenario_id}" in line
                            ):
                                capture = True

                            if capture:
                                scenario_test_code.append(line)

                            if capture and "END;" in line:
                                # Assuming END; marks the end of a test procedure
                                break

                        if scenario_test_code:
                            scenario["testCode"] = "\n".join(scenario_test_code)

                # Look for test results
                results_dir = f"output/sql-tests/{procedure}/results"

                if os.path.exists(results_dir):
                    # First, collect all result and error files
                    all_result_files = {}
                    all_error_files = {}
                    all_test_data_files = {}

                    for filename in os.listdir(results_dir):
                        file_path = os.path.join(results_dir, filename)

                        if filename.endswith("_tsqlt_results.json"):
                            with open(file_path, "r") as results_file:
                                try:
                                    results = json.load(results_file)
                                    all_result_files[filename] = results
                                except json.JSONDecodeError:
                                    print(f"⚠️ Error parsing results file: {filename}")

                        elif filename.endswith("_error.json"):
                            with open(file_path, "r") as error_file:
                                try:
                                    error_data = json.load(error_file)
                                    all_error_files[filename] = error_data
                                except json.JSONDecodeError:
                                    print(f"⚠️ Error parsing error file: {filename}")

                        elif filename.endswith("_test_results.json"):
                            with open(file_path, "r") as test_data_file:
                                try:
                                    test_data = json.load(test_data_file)
                                    all_test_data_files[filename] = test_data
                                except json.JSONDecodeError:
                                    print(f"⚠️ Error parsing test data file: {filename}")

                # Now process all business processes and their scenarios
                for bp in discoveredBusinessProcesses:
                    for scenario in bp["testScenarios"]:
                        scenario_id = scenario["scenarioId"]

                        # Check for error files first
                        for filename, error_data in all_error_files.items():
                            if scenario_id in filename:
                                scenario["error"] = error_data

                        # Check for matching test results
                        matching_results = []

                        for filename, results in all_result_files.items():
                            for result in results:
                                # Check if the TestCase field contains this scenario's ID
                                test_case = result.get("TestCase", "")

                                # Handle different formats and variations
                                # Extract the scenario ID without dashes or underscores
                                scenario_number = ""
                                if "-" in scenario_id:
                                    scenario_number = scenario_id.split("-")[1]
                                elif "_" in scenario_id:
                                    scenario_number = scenario_id.split("_")[1]
                                else:
                                    # Try to extract numeric part if it's like SCEN001
                                    import re

                                    match = re.search(r"SCEN(\d+)", scenario_id)
                                    if match:
                                        scenario_number = match.group(1)

                                # Check for various formats of the scenario ID in the test case
                                is_match = False

                                # Direct match
                                if scenario_id in test_case:
                                    is_match = True
                                # Match with different separators
                                elif (
                                    f"SCEN-{scenario_number}" in test_case
                                    or f"SCEN_{scenario_number}" in test_case
                                ):
                                    is_match = True
                                # Match without separator
                                elif f"SCEN{scenario_number}" in test_case:
                                    is_match = True
                                # Match with variation but still related to this scenario
                                elif (
                                    f"SCEN{scenario_number}_VAR" in test_case
                                    or f"SCEN-{scenario_number}_VAR" in test_case
                                    or f"SCEN_{scenario_number}_VAR" in test_case
                                ):
                                    is_match = True
                                # Match with variation using different separator
                                elif (
                                    f"SCEN{scenario_number}-VAR" in test_case
                                    or f"SCEN-{scenario_number}-VAR" in test_case
                                    or f"SCEN_{scenario_number}-VAR" in test_case
                                ):
                                    is_match = True

                                if is_match:
                                    matching_results.append(result)

                        if matching_results:
                            scenario["testResults"] = matching_results

                        # Similarly, update the test data snapshots matching logic
                        matching_snapshots = {}

                        for filename, test_data in all_test_data_files.items():
                            # Extract the scenario number from the filename
                            scenario_number = ""
                            if "-" in scenario_id:
                                scenario_number = scenario_id.split("-")[1]
                            elif "_" in scenario_id:
                                scenario_number = scenario_id.split("_")[1]
                            else:
                                # Try to extract numeric part if it's like SCEN001
                                import re

                                match = re.search(r"SCEN(\d+)", scenario_id)
                                if match:
                                    scenario_number = match.group(1)

                            # Check for various formats in the filename
                            is_match = False

                            # Direct match
                            if scenario_id in filename:
                                is_match = True
                            # Match with different separators
                            elif (
                                f"SCEN-{scenario_number}" in filename
                                or f"SCEN_{scenario_number}" in filename
                            ):
                                is_match = True
                            # Match without separator
                            elif f"SCEN{scenario_number}" in filename:
                                is_match = True
                            # Match with variation but still related to this scenario
                            elif (
                                f"SCEN{scenario_number}_VAR" in filename
                                or f"SCEN-{scenario_number}_VAR" in filename
                                or f"SCEN_{scenario_number}_VAR" in filename
                            ):
                                is_match = True
                            # Match with variation using different separator
                            elif (
                                f"SCEN{scenario_number}-VAR" in filename
                                or f"SCEN-{scenario_number}-VAR" in filename
                                or f"SCEN_{scenario_number}-VAR" in filename
                            ):
                                is_match = True

                            if is_match:
                                # Store with the variation name as key if it's a variation
                                variation_key = "main"
                                import re

                                var_match = re.search(r"VAR(\d+)", filename)
                                if var_match:
                                    variation_key = f"VAR{var_match.group(1)}"

                                matching_snapshots[variation_key] = test_data

                        # Process all matching snapshots
                        if matching_snapshots:
                            all_processed_test_data = []

                            for variation_key, test_data in matching_snapshots.items():
                                # Process the test data to organize by table name
                                processed_test_data = []

                                # First, collect all tables and their data
                                all_tables = {}
                                for snapshot_group in test_data:
                                    if isinstance(snapshot_group, list):
                                        for row in snapshot_group:
                                            if (
                                                isinstance(row, dict)
                                                and "table_name" in row
                                            ):
                                                table_name = row["table_name"]
                                                if table_name not in all_tables:
                                                    all_tables[table_name] = []
                                                all_tables[table_name].append(row)

                                # Now organize tables in a logical order
                                # First, extract base names (without Before/After prefix)
                                base_tables = {}
                                for table_name in all_tables.keys():
                                    # Remove # if present
                                    clean_name = table_name.replace("#", "")

                                    # Check if it starts with Before or After
                                    if clean_name.startswith("Before"):
                                        base_name = clean_name[6:]  # Remove "Before"
                                        if base_name not in base_tables:
                                            base_tables[base_name] = {
                                                "before": None,
                                                "after": None,
                                            }
                                        base_tables[base_name]["before"] = table_name
                                    elif clean_name.startswith("After"):
                                        base_name = clean_name[5:]  # Remove "After"
                                        if base_name not in base_tables:
                                            base_tables[base_name] = {
                                                "before": None,
                                                "after": None,
                                            }
                                        base_tables[base_name]["after"] = table_name
                                    else:
                                        # For tables without Before/After prefix
                                        if clean_name not in base_tables:
                                            base_tables[clean_name] = {
                                                "other": table_name
                                            }

                                # Now add tables in the desired order
                                for base_name, tables in base_tables.items():
                                    # Add Before table first if it exists
                                    if "before" in tables and tables["before"]:
                                        before_table = tables["before"]
                                        processed_test_data.append(
                                            {
                                                "tableName": before_table,
                                                "rows": all_tables[before_table],
                                            }
                                        )

                                    # Add After table next if it exists
                                    if "after" in tables and tables["after"]:
                                        after_table = tables["after"]
                                        processed_test_data.append(
                                            {
                                                "tableName": after_table,
                                                "rows": all_tables[after_table],
                                            }
                                        )

                                    # Add other tables if they exist
                                    if "other" in tables:
                                        other_table = tables["other"]
                                        processed_test_data.append(
                                            {
                                                "tableName": other_table,
                                                "rows": all_tables[other_table],
                                            }
                                        )

                                # Add any remaining tables that weren't categorized
                                for table_name, rows in all_tables.items():
                                    if not any(
                                        table["tableName"] == table_name
                                        for table in processed_test_data
                                    ):
                                        processed_test_data.append(
                                            {
                                                "tableName": table_name,
                                                "rows": rows,
                                            }
                                        )

                                # Add variation information if it's not the main test
                                if variation_key != "main":
                                    all_processed_test_data.append(
                                        {
                                            "variation": variation_key,
                                            "tables": processed_test_data,
                                        }
                                    )
                                else:
                                    all_processed_test_data.append(
                                        {"tables": processed_test_data}
                                    )

                            scenario["testDataSnapshots"] = all_processed_test_data

        # After processing all scenarios, save the result to a JSON file
        output_dir = f"output/analysis/{procedure}/processed"
        os.makedirs(output_dir, exist_ok=True)

        with open(f"{output_dir}/business_processes_with_scenarios.json", "w") as f:
            json.dump({"businessProcesses": discoveredBusinessProcesses}, f, indent=4)

        print(f"✅ Created business processes with scenarios JSON for {procedure}")


def generate_markdown_from_json(procedure):
    """Generate a markdown file from the business processes JSON"""

    output_dir = f"output/analysis/{procedure}/processed"
    json_file_path = f"{output_dir}/business_processes_with_scenarios.json"
    markdown_file_path = f"{output_dir}/{procedure}_test_report.md"

    # Read the JSON file
    with open(json_file_path, "r") as f:
        data = json.load(f)

    business_processes = data.get("businessProcesses", [])

    # Start building the markdown content
    markdown_content = f"# Test Report for {procedure}\n\n"
    markdown_content += (
        f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    )

    # Add a table of contents
    markdown_content += "## Table of Contents\n\n"
    for bp in business_processes:
        bp_id = bp.get("processId", "Unknown")
        bp_name = bp.get("processName", "Unknown")
        markdown_content += f"- [{bp_id} - {bp_name}](#{bp_id.lower()}-{bp_name.lower().replace(' ', '-')})\n"

    markdown_content += "\n---\n\n"

    # Add details for each business process
    for bp in business_processes:
        bp_id = bp.get("processId", "Unknown")
        bp_name = bp.get("processName", "Unknown")
        bp_description = bp.get("processDescription", "No description available")

        markdown_content += f"## {bp_id} - {bp_name}\n\n"
        markdown_content += f"**Description**: {bp_description}\n\n"

        # Add a summary table of test scenarios
        markdown_content += "### Test Scenarios Summary\n\n"
        markdown_content += "| Scenario ID | Name | Result | Details |\n"
        markdown_content += "|------------|------|--------|--------|\n"

        for scenario in bp.get("testScenarios", []):
            scenario_id = scenario.get("scenarioId", "Unknown")
            scenario_name = scenario.get("scenarioName", "Unknown")

            # Determine the test result
            result = "Not Executed"
            details = ""

            if "error" in scenario:
                result = "❌ Error"
                error_msg = scenario["error"].get("error", "Unknown error")
                details = f"Error occurred"

            if "testResults" in scenario:
                for test_result in scenario["testResults"]:
                    if test_result.get("Result") == "Success":
                        result = "✅ Success"
                        details = f"Executed successfully"
                    elif test_result.get("Result") == "Error":
                        result = "❌ Error"
                        details = f"Test execution failed"
                    else:
                        result = f"⚠️ {test_result.get('Result', 'Unknown')}"
                        details = f"See details below"

            markdown_content += (
                f"| {scenario_id} | {scenario_name} | {result} | {details} |\n"
            )

        markdown_content += "\n"

        # Add detailed information for each test scenario
        markdown_content += "### Detailed Test Scenarios\n\n"

        for scenario in bp.get("testScenarios", []):
            scenario_id = scenario.get("scenarioId", "Unknown")
            scenario_name = scenario.get("scenarioName", "Unknown")
            scenario_description = scenario.get(
                "scenarioDescription", "No description available"
            )

            markdown_content += f"#### {scenario_id}: {scenario_name}\n\n"
            markdown_content += f"**Description**: {scenario_description}\n\n"

            # Add test code if available
            if "testCode" in scenario:
                markdown_content += "**Test Code**:\n\n```sql\n"
                markdown_content += scenario["testCode"]
                markdown_content += "\n```\n\n"

            # Add test results if available
            if "testResults" in scenario:
                markdown_content += "**Test Results**:\n\n"

                for test_result in scenario["testResults"]:
                    result_status = test_result.get("Result", "Unknown")
                    test_case = test_result.get("TestCase", "Unknown")
                    test_start = test_result.get("TestStartTime", "Unknown")
                    test_end = test_result.get("TestEndTime", "Unknown")

                    markdown_content += f"- **Status**: {result_status}\n"
                    markdown_content += f"- **Test Case**: {test_case}\n"
                    markdown_content += f"- **Started**: {test_start}\n"
                    markdown_content += f"- **Completed**: {test_end}\n"

                    if test_result.get("Msg"):
                        markdown_content += f"- **Message**: {test_result.get('Msg')}\n"

                    markdown_content += "\n"

            # Add error information if available
            if "error" in scenario:
                markdown_content += "**Error Information**:\n\n```\n"
                markdown_content += str(scenario["error"].get("error", "Unknown error"))
                markdown_content += "\n```\n\n"

            # Add test data snapshots if available
            if "testDataSnapshots" in scenario:
                markdown_content += "**Test Data Snapshots**:\n\n"

                # Process each snapshot set (main test and variations)
                for snapshot_set in scenario["testDataSnapshots"]:
                    # Check if this is a variation
                    if "variation" in snapshot_set:
                        markdown_content += (
                            f"**Variation: {snapshot_set['variation']}**\n\n"
                        )

                    # Group tables by their base name for better organization
                    base_table_groups = {}

                    for table_data in snapshot_set.get("tables", []):
                        table_name = table_data.get("tableName", "Unknown")

                        # Extract base name for grouping
                        clean_name = table_name.replace("#", "")
                        base_name = None

                        if clean_name.startswith("Before"):
                            base_name = clean_name[6:]
                        elif clean_name.startswith("After"):
                            base_name = clean_name[5:]
                        else:
                            base_name = clean_name

                        if base_name not in base_table_groups:
                            base_table_groups[base_name] = []

                        base_table_groups[base_name].append(table_data)

                    # Now display tables grouped by their base name
                    for base_name, tables in base_table_groups.items():
                        # Sort tables to ensure Before comes before After
                        tables.sort(
                            key=lambda x: (
                                "1"
                                if "Before" in x["tableName"]
                                else "2" if "After" in x["tableName"] else "3"
                            )
                        )

                        for table_data in tables:
                            table_name = table_data.get("tableName", "Unknown")
                            rows = table_data.get("rows", [])

                            markdown_content += f"**{table_name}**:\n\n"

                            if rows:
                                # Get all unique keys from all rows
                                all_keys = set()
                                for row in rows:
                                    for key in row.keys():
                                        if (
                                            key != "table_name"
                                        ):  # Skip the table_name field
                                            all_keys.add(key)

                                # Sort keys for consistent display
                                headers = sorted(list(all_keys))

                                # Create the table header
                                markdown_content += "| " + " | ".join(headers) + " |\n"
                                markdown_content += (
                                    "| " + " | ".join(["---"] * len(headers)) + " |\n"
                                )

                                # Add each row
                                for row in rows:
                                    row_values = []
                                    for header in headers:
                                        value = row.get(header, "")
                                        # Format the value for markdown
                                        if value is None:
                                            value = "NULL"
                                        elif isinstance(value, str):
                                            # Escape pipe characters in strings
                                            value = str(value).replace("|", "\\|")
                                        row_values.append(str(value))

                                    markdown_content += (
                                        "| " + " | ".join(row_values) + " |\n"
                                    )
                            else:
                                markdown_content += "*No data*\n"

                            markdown_content += "\n"

                    # Add a separator between variations
                    if "variation" in snapshot_set:
                        markdown_content += "---\n\n"

            markdown_content += "---\n\n"

    # Write the markdown content to a file
    with open(markdown_file_path, "w") as f:
        f.write(markdown_content)

    print(f"✅ Created markdown report at {markdown_file_path}")


# Add this to your main code after saving the JSON
import datetime

for procedure in procedures:
    # After the JSON has been created
    generate_markdown_from_json(procedure)
