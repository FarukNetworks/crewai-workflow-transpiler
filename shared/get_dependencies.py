import os
import json


def get_dependencies(procedure_name):
    # Get dependencies
    with open(os.path.join("output/data", "procedure_dependencies.json"), "r") as f:
        all_procedures = json.load(f)

    # Find the procedure with matching name in the dependencies list
    procedure_dependencies = []
    for proc in all_procedures:
        if proc.get("name") == procedure_name:
            procedure_dependencies = proc.get("dependencies", [])
            break

    # Print a summary of the dependencies
    print(f"Dependencies for {procedure_name}:")
    print("-" * 50)

    # Group dependencies by type
    tables = []
    views = []
    functions = []
    procedures = []
    other = []

    for dep in procedure_dependencies:
        dep_type = dep.get("type", "UNKNOWN")

        if dep_type == "TABLE":
            tables.append(dep)
        elif dep_type == "VIEW":
            views.append(dep)
        elif dep_type == "FUNCTION":
            functions.append(dep)
        elif dep_type == "PROCEDURE":
            procedures.append(dep)
        else:
            other.append(dep)

    # Print tables
    if tables:
        print(f"\nTABLES ({len(tables)}):")
        for table in tables:
            print(f"  - {table['name']}")
            if "columns" in table:
                print(f"    Columns:")
                for col in table["columns"]:
                    nullable = "NULL" if col["is_nullable"] else "NOT NULL"
                    print(f"      - {col['name']} ({col['data_type']}) {nullable}")

    # Print views
    if views:
        print(f"\nVIEWS ({len(views)}):")
        for view in views:
            print(f"  - {view['name']}")
            if "columns" in view:
                print(f"    Columns:")
                for col in view["columns"]:
                    nullable = "NULL" if col["is_nullable"] else "NOT NULL"
                    print(f"      - {col['name']} ({col['data_type']}) {nullable}")

    # Print functions
    if functions:
        print(f"\nFUNCTIONS ({len(functions)}):")
        for func in functions:
            print(f"  - {func['name']}")
            if "definition" in func:
                print(
                    f"    Definition available: {'Yes' if func['definition'] else 'No'}"
                )

    # Print procedures
    if procedures:
        print(f"\nPROCEDURES ({len(procedures)}):")
        for proc in procedures:
            print(f"  - {proc['name']}")

    # Print other dependencies
    if other:
        print(f"\nOTHER DEPENDENCIES ({len(other)}):")
        for dep in other:
            print(f"  - {dep['name']} (Type: {dep.get('type', 'UNKNOWN')})")

    # Check for non-existent dependencies
    non_existent = [dep for dep in procedure_dependencies if dep.get("exists") is False]
    if non_existent:
        print(f"\nNON-EXISTENT DEPENDENCIES ({len(non_existent)}):")
        for dep in non_existent:
            print(f"  - {dep['name']} (Type: {dep.get('type', 'UNKNOWN')})")

    # Save the detailed dependencies to a JSON file for this specific procedure
    output_dir = f"output/analysis/{procedure_name}"
    os.makedirs(output_dir, exist_ok=True)

    with open(f"{output_dir}/{procedure_name}_dependencies.json", "w") as f:
        json.dump(
            {
                "name": procedure_name,
                "dependencies": {
                    "tables": tables,
                    "views": views,
                    "functions": functions,
                    "procedures": procedures,
                    "other": other,
                    "non_existent": non_existent,
                },
            },
            f,
            indent=4,
        )

    return procedure_dependencies
