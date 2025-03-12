import os
import dotenv
import pyodbc
import json


dotenv.load_dotenv()

connection_string = os.getenv("CONNECTION_STRING")

connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

# Get all stored procedures
cursor.execute(
    """
SELECT 
    s.name + '.' + p.name AS name
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE s.name NOT LIKE '%tSQLt%' 
  AND p.name NOT LIKE '%tSQLt%'
ORDER BY s.name, p.name;
"""
)
procedures = cursor.fetchall()

# Get Each procedure dependencies
procedure_dependencies = []

for procedure in procedures:
    procedure_name = (
        procedure.name.split(".")[1] if "." in procedure.name else procedure.name
    )
    schema_name = procedure.name.split(".")[0] if "." in procedure.name else "dbo"
    full_procedure_name = procedure.name

    # Get table dependencies
    cursor.execute(
        f"""
    SELECT 
        ISNULL(referenced_schema_name, 'dbo') + '.' + referenced_entity_name AS table_name
    FROM sys.sql_expression_dependencies d
    JOIN sys.objects o ON d.referencing_id = o.object_id
    JOIN sys.tables t ON d.referenced_id = t.object_id
    WHERE OBJECT_ID('{full_procedure_name}') = d.referencing_id
    """
    )

    dependencies = cursor.fetchall()
    dependency_list = []

    for dep in dependencies:
        table_name = dep.table_name

        # Get column metadata for each dependency
        cursor.execute(
            f"""
        SELECT 
            c.name AS column_name,
            t.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID('{table_name}')
        ORDER BY c.column_id
        """
        )

        columns = cursor.fetchall()
        column_metadata = [
            {
                "name": col.column_name,
                "data_type": col.data_type,
                "max_length": col.max_length,
                "precision": col.precision,
                "scale": col.scale,
                "is_nullable": col.is_nullable,
            }
            for col in columns
        ]

        dependency_list.append({"table_name": table_name, "columns": column_metadata})

    procedure_dependencies.append(
        {"name": full_procedure_name, "dependencies": dependency_list}
    )
    print(
        f"Processed procedure: {full_procedure_name} with {len(dependencies)} dependencies"
    )

# Save procedures to JSON file
with open("output/data/procedure_dependencies.json", "w") as f:
    json.dump(procedure_dependencies, f, indent=4)

print("Procedure discovery completed.")

# Close the database connection
connection.close()
