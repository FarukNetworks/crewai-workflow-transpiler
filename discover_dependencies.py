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

    # Get all dependencies (tables, views, functions, procedures, triggers)
    cursor.execute(
        f"""
    SELECT 
        ISNULL(OBJECT_SCHEMA_NAME(d.referenced_id), 'dbo') + '.' + OBJECT_NAME(d.referenced_id) AS referenced_name,
        o.type_desc AS object_type
    FROM sys.sql_expression_dependencies d
    JOIN sys.objects o ON d.referenced_id = o.object_id
    WHERE OBJECT_ID('{full_procedure_name}') = d.referencing_id
    AND d.referenced_id IS NOT NULL
    """
    )

    dependencies = cursor.fetchall()
    dependency_list = []

    for dep in dependencies:
        referenced_name = dep.referenced_name
        object_type = dep.object_type

        # Process based on object type
        if object_type == "USER_TABLE":
            # Get column metadata for tables
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
            WHERE c.object_id = OBJECT_ID('{referenced_name}')
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

            dependency_list.append(
                {"name": referenced_name, "type": "TABLE", "columns": column_metadata}
            )

        elif object_type == "VIEW":
            # Get column metadata for views
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
            WHERE c.object_id = OBJECT_ID('{referenced_name}')
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

            dependency_list.append(
                {"name": referenced_name, "type": "VIEW", "columns": column_metadata}
            )

        elif object_type in (
            "SQL_STORED_PROCEDURE",
            "SQL_INLINE_TABLE_VALUED_FUNCTION",
            "SQL_SCALAR_FUNCTION",
            "SQL_TABLE_VALUED_FUNCTION",
        ):
            # For procedures and functions, just store the reference
            dependency_type = (
                "PROCEDURE" if object_type == "SQL_STORED_PROCEDURE" else "FUNCTION"
            )

            # For functions, get the definition
            if dependency_type == "FUNCTION":
                cursor.execute(
                    f"""
                SELECT 
                    m.definition
                FROM sys.sql_modules m
                WHERE m.object_id = OBJECT_ID('{referenced_name}')
                """
                )
                definition_row = cursor.fetchone()
                definition = definition_row.definition if definition_row else None

                dependency_list.append(
                    {
                        "name": referenced_name,
                        "type": dependency_type,
                        "definition": definition,
                    }
                )
            else:
                dependency_list.append(
                    {"name": referenced_name, "type": dependency_type}
                )

        elif object_type == "SQL_TRIGGER":
            dependency_list.append({"name": referenced_name, "type": "TRIGGER"})

        else:
            # For any other object types
            dependency_list.append({"name": referenced_name, "type": object_type})

    procedure_dependencies.append(
        {"name": full_procedure_name, "dependencies": dependency_list}
    )
    print(
        f"Processed procedure: {full_procedure_name} with {len(dependencies)} dependencies"
    )

# Save procedures to JSON file
os.makedirs("output/data", exist_ok=True)
with open("output/data/procedure_dependencies.json", "w") as f:
    json.dump(procedure_dependencies, f, indent=4)

print("Procedure discovery completed.")

# Close the database connection
connection.close()
