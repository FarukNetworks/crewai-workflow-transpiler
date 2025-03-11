"""
SQL Operations Detector

Detects and analyzes data operations, dynamic SQL, and temporary table usage in SQL stored procedures.
"""

import re
import logging
import uuid
from typing import Dict, List, Optional, Any, Tuple, Set
import sqlparse
from sqlparse.sql import Token, TokenList

logger = logging.getLogger(__name__)


class OperationDetector:
    """
    Detects SQL operations, including DML statements, dynamic SQL, and temporary tables.
    """
    
    def __init__(self, sql_content: str, logical_blocks: List[Dict[str, Any]]):
        """
        Initialize with SQL content and identified logical blocks
        
        Args:
            sql_content: The SQL stored procedure code
            logical_blocks: List of logical blocks from StructureAnalyzer
        """
        self.sql_content = sql_content
        self.logical_blocks = logical_blocks
        self.parsed_statements = sqlparse.parse(sql_content)
        self.table_references = []
        self.dynamic_sql_operations = []
        self.temp_structures = []
        self.data_flows = []
        
        # Track detected table and column names
        self.detected_tables = set()
        self.detected_columns = {}  # Table -> [Columns]
        
        # Track temp tables specifically
        self.temp_tables = set()
        self.table_variables = set()
    
    def detect_table_references(self) -> List[Dict[str, Any]]:
        """
        Detect tables and columns referenced in DML operations
        
        Returns:
            List of table references with operation type, columns, etc.
        """
        self.table_references = []
        
        # Analyze each statement
        for statement in self.parsed_statements:
            self._analyze_statement(statement)
        
        # Link table references to blocks
        self._link_references_to_blocks()
        
        return self.table_references
    
    def _analyze_statement(self, statement):
        """Analyze a SQL statement for table references and operations"""
        # Get statement type
        statement_type = self._get_statement_type(statement)
        
        if statement_type in ["SELECT", "INSERT", "UPDATE", "DELETE"]:
            # Extract regular SQL operations
            self._extract_dml_operations(statement, statement_type)
        elif self._is_dynamic_sql(statement):
            # Extract dynamic SQL
            self._extract_dynamic_sql(statement)
        elif self._is_temp_table_creation(statement):
            # Extract temp table definition
            self._extract_temp_table(statement)
    
    def _get_statement_type(self, statement):
        """Determine the type of SQL statement"""
        for token in statement.tokens:
            if token.is_keyword:
                upper_val = token.value.upper()
                if upper_val in ["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"]:
                    return upper_val
        return None
    
    def _extract_dml_operations(self, statement, statement_type):
        """Extract table and column references from DML statements"""
        if statement_type == "SELECT":
            self._extract_select_references(statement)
        elif statement_type == "INSERT":
            self._extract_insert_references(statement)
        elif statement_type == "UPDATE":
            self._extract_update_references(statement)
        elif statement_type == "DELETE":
            self._extract_delete_references(statement)
        elif statement_type == "MERGE":
            self._extract_merge_references(statement)
    
    def _extract_select_references(self, statement):
        """Extract tables and columns from SELECT statements"""
        # Extract FROM clause
        from_seen = False
        tables = []
        columns = []
        
        for token in statement.tokens:
            if token.is_keyword and token.value.upper() == "FROM":
                from_seen = True
            elif from_seen and token.ttype is None:
                # This should contain table references
                table_tokens = self._tokenize_table_references(token)
                tables.extend(table_tokens)
                from_seen = False
        
        # Extract SELECT columns
        select_seen = False
        for token in statement.tokens:
            if token.is_keyword and token.value.upper() == "SELECT":
                select_seen = True
            elif select_seen and token.ttype is None and token.value.upper() != "FROM":
                # This should contain column references
                column_tokens = self._tokenize_column_references(token)
                columns.extend(column_tokens)
                select_seen = False
        
        # Create table reference entry for each table
        for table in tables:
            table_ref = {
                "table": table,
                "operation": "SELECT",
                "columns": columns,
                "blockId": None  # Will be set in _link_references_to_blocks
            }
            self.table_references.append(table_ref)
            self.detected_tables.add(table)
    
    def _extract_insert_references(self, statement):
        """Extract tables and columns from INSERT statements"""
        # Extract INTO clause
        into_seen = False
        table = None
        columns = []
        
        for token in statement.tokens:
            if token.is_keyword and token.value.upper() == "INTO":
                into_seen = True
            elif into_seen and token.ttype is None:
                # This should contain the table name
                if "(" in token.value:
                    # Table with column list: "table_name (col1, col2, ...)"
                    parts = token.value.split("(", 1)
                    table = parts[0].strip().strip('[]"')
                    cols_part = "(" + parts[1]
                    col_matches = re.findall(r'[\[\"]?(\w+)[\]\"]?', cols_part)
                    columns.extend(col_matches)
                else:
                    # Just the table name
                    table = token.value.strip().strip('[]"')
                into_seen = False
        
        # Create table reference entry
        if table:
            table_ref = {
                "table": table,
                "operation": "INSERT",
                "columns": columns,
                "blockId": None  # Will be set in _link_references_to_blocks
            }
            self.table_references.append(table_ref)
            self.detected_tables.add(table)
            
            # Check if this is a temp table
            if table.startswith('#'):
                self.temp_tables.add(table)
    
    def _extract_update_references(self, statement):
        """Extract tables and columns from UPDATE statements"""
        # Extract table name
        update_seen = False
        table = None
        set_columns = []
        
        for token in statement.tokens:
            if token.is_keyword and token.value.upper() == "UPDATE":
                update_seen = True
            elif update_seen and token.ttype is None:
                # This should be the table name
                table = token.value.strip().strip('[]"')
                update_seen = False
        
        # Extract SET clause
        set_seen = False
        for token in statement.tokens:
            if token.is_keyword and token.value.upper() == "SET":
                set_seen = True
            elif set_seen and token.ttype is None:
                # This should contain column assignments
                assignments = token.value.split(',')
                for assignment in assignments:
                    if '=' in assignment:
                        column = assignment.split('=')[0].strip().strip('[]"')
                        set_columns.append(column)
                set_seen = False
        
        # Create table reference entry
        if table:
            table_ref = {
                "table": table,
                "operation": "UPDATE",
                "columns": set_columns,
                "blockId": None  # Will be set in _link_references_to_blocks
            }
            self.table_references.append(table_ref)
            self.detected_tables.add(table)
    
    def _extract_delete_references(self, statement):
        """Extract tables from DELETE statements"""
        # Extract FROM clause
        from_seen = False
        table = None
        
        for token in statement.tokens:
            if token.is_keyword and token.value.upper() == "FROM":
                from_seen = True
            elif from_seen and token.ttype is None:
                # This should be the table name
                table = token.value.strip().strip('[]"')
                from_seen = False
        
        # Create table reference entry
        if table:
            table_ref = {
                "table": table,
                "operation": "DELETE",
                "columns": [],
                "blockId": None  # Will be set in _link_references_to_blocks
            }
            self.table_references.append(table_ref)
            self.detected_tables.add(table)
    
    def _extract_merge_references(self, statement):
        """Extract tables from MERGE statements"""
        # Extract target table
        into_seen = False
        target_table = None
        
        for token in statement.tokens:
            if token.is_keyword and token.value.upper() == "INTO":
                into_seen = True
            elif into_seen and token.ttype is None:
                # This should be the target table name
                target_table = token.value.strip().strip('[]"')
                into_seen = False
        
        # Extract source table
        using_seen = False
        source_table = None
        
        for token in statement.tokens:
            if token.is_keyword and token.value.upper() == "USING":
                using_seen = True
            elif using_seen and token.ttype is None:
                # This should be the source table name
                source_table = token.value.strip().strip('[]"')
                using_seen = False
        
        # Create table reference entries
        if target_table:
            table_ref = {
                "table": target_table,
                "operation": "MERGE_TARGET",
                "columns": [],
                "blockId": None  # Will be set in _link_references_to_blocks
            }
            self.table_references.append(table_ref)
            self.detected_tables.add(target_table)
        
        if source_table:
            table_ref = {
                "table": source_table,
                "operation": "MERGE_SOURCE",
                "columns": [],
                "blockId": None  # Will be set in _link_references_to_blocks
            }
            self.table_references.append(table_ref)
            self.detected_tables.add(source_table)
    
    def _tokenize_table_references(self, token):
        """Extract table names from a token that contains FROM clause content"""
        if hasattr(token, 'tokens'):
            # Token is a TokenList
            tables = []
            for subtoken in token.tokens:
                if subtoken.ttype is None and not subtoken.is_whitespace:
                    # Handle table references with aliases
                    parts = re.split(r'\s+(?:AS\s+)?', subtoken.value, 1, re.IGNORECASE)
                    table_name = parts[0].strip().strip('[]"')
                    tables.append(table_name)
            return tables
        else:
            # Token is a simple token
            parts = re.split(r'\s+(?:AS\s+)?', token.value, 1, re.IGNORECASE)
            table_name = parts[0].strip().strip('[]"')
            return [table_name]
    
    def _tokenize_column_references(self, token):
        """Extract column names from a token that contains SELECT clause content"""
        if hasattr(token, 'tokens'):
            # Token is a TokenList
            columns = []
            for subtoken in token.tokens:
                if subtoken.ttype is None and not subtoken.is_whitespace:
                    # Handle column references with aliases
                    parts = re.split(r'\s+(?:AS\s+)?', subtoken.value, 1, re.IGNORECASE)
                    column_expr = parts[0].strip()
                    
                    # Extract simple column names, ignoring expressions
                    if not any(op in column_expr for op in ['+', '-', '*', '/', '(', ')']):
                        if '.' in column_expr:
                            # Handle table.column notation
                            col_parts = column_expr.split('.')
                            column_name = col_parts[-1].strip('[]"')
                        else:
                            column_name = column_expr.strip('[]"')
                        columns.append(column_name)
            return columns
        else:
            # Token is a simple token
            column_exprs = token.value.split(',')
            columns = []
            for expr in column_exprs:
                parts = re.split(r'\s+(?:AS\s+)?', expr, 1, re.IGNORECASE)
                column_expr = parts[0].strip()
                
                # Extract simple column names, ignoring expressions
                if not any(op in column_expr for op in ['+', '-', '*', '/', '(', ')']):
                    if '.' in column_expr:
                        # Handle table.column notation
                        col_parts = column_expr.split('.')
                        column_name = col_parts[-1].strip('[]"')
                    else:
                        column_name = column_expr.strip('[]"')
                    columns.append(column_name)
            return columns
    
    def _is_dynamic_sql(self, statement):
        """Check if a statement contains dynamic SQL execution"""
        # Look for EXEC or sp_executesql
        for token in statement.tokens:
            if token.ttype is None and hasattr(token, 'tokens'):
                for subtoken in token.tokens:
                    if subtoken.is_keyword and subtoken.value.upper() in ["EXEC", "EXECUTE"]:
                        return True
            elif token.is_keyword and token.value.upper() in ["EXEC", "EXECUTE"]:
                return True
        
        # Look for string assignments that contain SQL keywords
        sql_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "WHERE", "JOIN"]
        set_seen = False
        for token in statement.tokens:
            if token.is_keyword and token.value.upper() == "SET":
                set_seen = True
            elif set_seen and token.ttype is None:
                # Check if assignment contains SQL
                for keyword in sql_keywords:
                    if keyword in token.value.upper():
                        return True
                set_seen = False
        
        return False
    
    def _is_temp_table_creation(self, statement):
        """Check if a statement creates a temporary table"""
        # Look for CREATE TABLE #temp or DECLARE @table_var TABLE
        create_seen = False
        declare_seen = False
        
        for token in statement.tokens:
            if token.is_keyword and token.value.upper() == "CREATE":
                create_seen = True
            elif token.is_keyword and token.value.upper() == "DECLARE":
                declare_seen = True
            elif create_seen and token.is_keyword and token.value.upper() == "TABLE":
                # Check next token for temp table name
                for i, next_token in enumerate(statement.tokens[statement.tokens.index(token)+1:]):
                    if next_token.ttype is None and not next_token.is_whitespace:
                        table_name = next_token.value.strip().split('(')[0].strip()
                        if table_name.startswith('#'):
                            return True
                        break
                create_seen = False
            elif declare_seen and token.ttype is None and not token.is_whitespace:
                # Check if this is a table variable declaration
                if "@" in token.value and "TABLE" in statement.value.upper():
                    return True
                declare_seen = False
        
        return False
    
    def _extract_dynamic_sql(self, statement):
        """Extract and analyze dynamic SQL execution"""
        dynamic_sql_id = f"dsql_{len(self.dynamic_sql_operations)}"
        
        # Initialize dynamic SQL operation entry
        dynamic_sql_op = {
            "id": dynamic_sql_id,
            "blockId": None,  # Will be set in _link_references_to_blocks
            "constructionPattern": "UNKNOWN",
            "parameters": [],
            "potentialQueries": [],
            "businessPurpose": ""
        }
        
        # Determine construction pattern
        if "EXEC(" in statement.value.upper() or "EXECUTE(" in statement.value.upper():
            dynamic_sql_op["constructionPattern"] = "DIRECT_EXEC"
        elif "SP_EXECUTESQL" in statement.value.upper():
            dynamic_sql_op["constructionPattern"] = "SP_EXECUTESQL"
        elif "EXEC " in statement.value.upper() or "EXECUTE " in statement.value.upper():
            dynamic_sql_op["constructionPattern"] = "EXEC_STATEMENT"
        elif "+=" in statement.value or "=" in statement.value and any(kw in statement.value.upper() for kw in ["SELECT", "INSERT", "UPDATE", "DELETE"]):
            dynamic_sql_op["constructionPattern"] = "STRING_BUILDING"
        
        # Extract parameters
        param_pattern = r'@(\w+)'
        for match in re.finditer(param_pattern, statement.value):
            param_name = match.group(1)
            # Avoid duplicates
            if not any(p["name"] == f"@{param_name}" for p in dynamic_sql_op["parameters"]):
                param_entry = {
                    "name": f"@{param_name}",
                    "usage": "PARAMETER"  # Default usage type
                }
                dynamic_sql_op["parameters"].append(param_entry)
        
        # Extract potential SQL queries
        if dynamic_sql_op["constructionPattern"] == "SP_EXECUTESQL":
            # For sp_executesql, extract the first parameter which is the SQL string
            query_pattern = r'sp_executesql\s+N?[\'"]([^\'"]+)[\'"]'
            match = re.search(query_pattern, statement.value, re.IGNORECASE)
            if match:
                query_text = match.group(1)
                self._analyze_embedded_sql(dynamic_sql_op, query_text)
        
        elif dynamic_sql_op["constructionPattern"] in ["EXEC_STATEMENT", "DIRECT_EXEC"]:
            # For EXEC/EXECUTE, extract the string or variable after the command
            query_pattern = r'(?:EXEC|EXECUTE)\s+(?:N?[\'"]([^\'"]+)[\'"]|@(\w+))'
            match = re.search(query_pattern, statement.value, re.IGNORECASE)
            if match:
                query_text = match.group(1) or f"@{match.group(2)}"
                self._analyze_embedded_sql(dynamic_sql_op, query_text)
        
        elif dynamic_sql_op["constructionPattern"] == "STRING_BUILDING":
            # For string building, analyze the variable assignments
            var_pattern = r'@(\w+)\s*=\s*N?[\'"]([^\'"]+)[\'"]'
            append_pattern = r'@(\w+)\s*\+=\s*N?[\'"]([^\'"]+)[\'"]'
            
            var_values = {}
            
            # Find initial values
            for match in re.finditer(var_pattern, statement.value, re.IGNORECASE):
                var_name = f"@{match.group(1)}"
                var_value = match.group(2)
                var_values[var_name] = var_value
            
            # Find appended values
            for match in re.finditer(append_pattern, statement.value, re.IGNORECASE):
                var_name = f"@{match.group(1)}"
                var_value = match.group(2)
                if var_name in var_values:
                    var_values[var_name] += var_value
                else:
                    var_values[var_name] = var_value
            
            # Analyze the constructed SQL in each variable
            for var_name, sql_text in var_values.items():
                if any(kw in sql_text.upper() for kw in ["SELECT", "INSERT", "UPDATE", "DELETE"]):
                    self._analyze_embedded_sql(dynamic_sql_op, sql_text, var_name)
        
        # Add the dynamic SQL operation
        self.dynamic_sql_operations.append(dynamic_sql_op)
    
    def _analyze_embedded_sql(self, dynamic_sql_op, sql_text, var_name=None):
        """Analyze embedded SQL string to identify potential tables and operations"""
        # Initialize potential query entry
        query_entry = {
            "pattern": sql_text[:100] + ("..." if len(sql_text) > 100 else ""),
            "potentialTables": [],
            "potentialOperations": []
        }
        
        # Identify SQL operation type
        if "SELECT" in sql_text.upper():
            query_entry["potentialOperations"].append("SELECT")
        if "INSERT" in sql_text.upper():
            query_entry["potentialOperations"].append("INSERT")
        if "UPDATE" in sql_text.upper():
            query_entry["potentialOperations"].append("UPDATE")
        if "DELETE" in sql_text.upper():
            query_entry["potentialOperations"].append("DELETE")
        if "MERGE" in sql_text.upper():
            query_entry["potentialOperations"].append("MERGE")
        
        # Extract potential tables
        # FROM clause
        from_pattern = r'FROM\s+(\w+(?:\.\w+)?)'
        for match in re.finditer(from_pattern, sql_text, re.IGNORECASE):
            table_name = match.group(1)
            if table_name.upper() not in ["DUAL", "SYSDATE"]:
                query_entry["potentialTables"].append(table_name)
        
        # INTO clause
        into_pattern = r'INTO\s+(\w+(?:\.\w+)?)'
        for match in re.finditer(into_pattern, sql_text, re.IGNORECASE):
            table_name = match.group(1)
            query_entry["potentialTables"].append(table_name)
        
        # UPDATE clause
        update_pattern = r'UPDATE\s+(\w+(?:\.\w+)?)'
        for match in re.finditer(update_pattern, sql_text, re.IGNORECASE):
            table_name = match.group(1)
            query_entry["potentialTables"].append(table_name)
        
        # Add the query entry
        if query_entry["potentialOperations"]:
            dynamic_sql_op["potentialQueries"].append(query_entry)
        
        # Update parameter usage if this is a variable
        if var_name:
            for param in dynamic_sql_op["parameters"]:
                if param["name"] == var_name:
                    param["usage"] = "SQL_CONTAINER"
                    break
    
    def _extract_temp_table(self, statement):
        """Extract temporary table structure"""
        # Initialize temp structure entry
        temp_id = f"temp_{len(self.temp_structures)}"
        
        # Determine if this is a table variable or temp table
        is_table_var = "DECLARE" in statement.value.upper()
        
        temp_structure = {
            "name": "",
            "type": "TABLE_VARIABLE" if is_table_var else "LOCAL_TEMP",
            "definition": statement.value[:200] + ("..." if len(statement.value) > 200 else ""),
            "columns": [],
            "populatedFrom": [],
            "usedIn": [],
            "transformations": []
        }
        
        # Extract the table name
        if is_table_var:
            # Table variable: DECLARE @table_name TABLE ...
            pattern = r'DECLARE\s+@(\w+)\s+TABLE'
            match = re.search(pattern, statement.value, re.IGNORECASE)
            if match:
                temp_structure["name"] = f"@{match.group(1)}"
                self.table_variables.add(temp_structure["name"])
        else:
            # Temp table: CREATE TABLE #table_name ...
            pattern = r'CREATE\s+TABLE\s+#(\w+)'
            match = re.search(pattern, statement.value, re.IGNORECASE)
            if match:
                temp_structure["name"] = f"#{match.group(1)}"
                self.temp_tables.add(temp_structure["name"])
        
        # Extract columns
        col_pattern = r'\(([^)]+)\)'
        match = re.search(col_pattern, statement.value, re.IGNORECASE)
        if match:
            col_defs = match.group(1).split(',')
            for col_def in col_defs:
                # Extract column name and data type
                col_match = re.search(r'^\s*(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?)', col_def.strip(), re.IGNORECASE)
                if col_match:
                    col_name = col_match.group(1)
                    col_type = col_match.group(2)
                    temp_structure["columns"].append({
                        "name": col_name,
                        "dataType": col_type
                    })
        
        # Add the temp structure
        if temp_structure["name"]:
            self.temp_structures.append(temp_structure)
    
    def analyze_dynamic_sql(self) -> List[Dict[str, Any]]:
        """
        Identify and analyze dynamic SQL within the stored procedure
        
        Returns:
            List of dynamic SQL operations
        """
        # Dynamic SQL operations are already populated in _extract_dynamic_sql
        # Just link them to blocks here
        self._link_dynamic_sql_to_blocks()
        return self.dynamic_sql_operations
    
    def analyze_temp_tables(self) -> List[Dict[str, Any]]:
        """
        Identify and analyze temporary tables within the stored procedure
        
        Returns:
            List of temporary table structures
        """
        # Analyze population and usage of temp tables
        self._analyze_temp_table_operations()
        return self.temp_structures
    
    def analyze_data_flows(self) -> List[Dict[str, Any]]:
        """
        Analyze data flows between tables
        
        Returns:
            List of data flow mappings
        """
        data_flows = []
        
        # Look for data flows from permanent to temp tables
        for temp_structure in self.temp_structures:
            flow_id = f"flow_{len(data_flows)}"
            
            # Find source tables that populate this temp structure
            source_tables = []
            for population in temp_structure["populatedFrom"]:
                if "tableReference" in population:
                    source_tables.append(population["tableReference"])
            
            # Find destination tables where this temp structure is used
            destination_tables = []
            for usage in temp_structure["usedIn"]:
                if "operation" in usage and usage["operation"].startswith("INSERT_INTO_"):
                    dest_table = usage["operation"].replace("INSERT_INTO_", "")
                    destination_tables.append(dest_table)
            
            # Create a data flow if we have both sources and destinations
            if source_tables and destination_tables:
                data_flow = {
                    "id": flow_id,
                    "sourceTables": source_tables,
                    "intermediateStructures": [temp_structure["name"]],
                    "destinationTables": destination_tables,
                    "transformationSequence": [t["type"] for t in temp_structure["transformations"]],
                    "businessPurpose": self._infer_business_purpose(temp_structure)
                }
                data_flows.append(data_flow)
        
        self.data_flows = data_flows
        return data_flows
    
    def _analyze_temp_table_operations(self):
        """Analyze operations involving temporary tables"""
        # Look for operations that populate temp tables
        for table_ref in self.table_references:
            table_name = table_ref["table"]
            
            # Check if this is an operation on a temp table
            if table_name in self.temp_tables or table_name in self.table_variables:
                temp_idx = self._find_temp_structure_index(table_name)
                if temp_idx >= 0:
                    # This is an operation on a known temp table
                    if table_ref["operation"] == "INSERT":
                        # This populates the temp table
                        # Find the corresponding SELECT that might be the source
                        select_refs = [ref for ref in self.table_references 
                                     if ref["operation"] == "SELECT" and 
                                     ref.get("blockId") == table_ref.get("blockId")]
                        
                        for select_ref in select_refs:
                            population = {
                                "tableReference": select_ref["table"],
                                "operation": "SELECT_INTO_TEMP",
                                "blockId": select_ref.get("blockId")
                            }
                            self.temp_structures[temp_idx]["populatedFrom"].append(population)
                    
                    elif table_ref["operation"] == "SELECT":
                        # This reads from the temp table
                        usage = {
                            "operation": "SELECT_FROM_TEMP",
                            "blockId": table_ref.get("blockId"),
                            "businessPurpose": ""
                        }
                        self.temp_structures[temp_idx]["usedIn"].append(usage)
                    
                    elif table_ref["operation"] == "UPDATE":
                        # This transforms data in the temp table
                        transformation = {
                            "type": "UPDATE",
                            "blockId": table_ref.get("blockId"),
                            "description": f"Update columns: {', '.join(table_ref['columns'])}"
                        }
                        self.temp_structures[temp_idx]["transformations"].append(transformation)
                    
                    elif table_ref["operation"] == "DELETE":
                        # This filters data in the temp table
                        transformation = {
                            "type": "FILTER",
                            "blockId": table_ref.get("blockId"),
                            "description": "Delete/filter rows"
                        }
                        self.temp_structures[temp_idx]["transformations"].append(transformation)
            
            # Check if this operation inserts into a permanent table from a temp table
            elif table_ref["operation"] == "INSERT":
                # Look for SELECT operations from temp tables that might be the source
                for temp_name in list(self.temp_tables) + list(self.table_variables):
                    temp_idx = self._find_temp_structure_index(temp_name)
                    if temp_idx >= 0:
                        # Check if there's a SELECT from this temp table in the same block
                        if any(ref["table"] == temp_name and 
                              ref["operation"] == "SELECT" and 
                              ref.get("blockId") == table_ref.get("blockId")
                              for ref in self.table_references):
                            
                            # This permanent table is populated from the temp table
                            usage = {
                                "operation": f"INSERT_INTO_{table_ref['table']}",
                                "blockId": table_ref.get("blockId"),
                                "businessPurpose": ""
                            }
                            self.temp_structures[temp_idx]["usedIn"].append(usage)
    
    def _find_temp_structure_index(self, temp_name):
        """Find the index of a temporary structure by name"""
        for i, structure in enumerate(self.temp_structures):
            if structure["name"] == temp_name:
                return i
        return -1
    


    def _infer_business_purpose(self, temp_structure):
        """Infer the business purpose of a temporary structure based on its operations"""
        # Basic heuristics to infer purpose
        transformations = [t["type"] for t in temp_structure["transformations"]]
        
        if not transformations:
            return "Data staging"
        
        if "AGGREGATE" in transformations:
            return "Data aggregation"
        
        if transformations.count("FILTER") > 1:
            return "Data filtering and validation"
        
        if "JOIN" in transformations:
            return "Data enrichment/combination"
        
        if "UPDATE" in transformations:
            return "Data transformation"
        
        return "Intermediate processing"
    
    def _link_references_to_blocks(self):
        """Link table references to their containing logical blocks"""
        for table_ref in self.table_references:
            # Look for table reference in each block
            for block in self.logical_blocks:
                # Check for exact table name match in the block code
                if re.search(r'\b' + re.escape(table_ref["table"]) + r'\b', block["codeText"], re.IGNORECASE):
                    # Check for operation match
                    op_keyword = table_ref["operation"]
                    if op_keyword in ["MERGE_TARGET", "MERGE_SOURCE"]:
                        op_keyword = "MERGE"
                    
                    if op_keyword in block["codeText"].upper():
                        table_ref["blockId"] = block["id"]
                        break
    
    def _link_dynamic_sql_to_blocks(self):
        """Link dynamic SQL operations to their containing logical blocks"""
        for dynamic_sql in self.dynamic_sql_operations:
            # Check for patterns of dynamic SQL in each block
            for block in self.logical_blocks:
                if dynamic_sql["constructionPattern"] == "DIRECT_EXEC" and "EXEC(" in block["codeText"].upper():
                    dynamic_sql["blockId"] = block["id"]
                    break
                elif dynamic_sql["constructionPattern"] == "SP_EXECUTESQL" and "SP_EXECUTESQL" in block["codeText"].upper():
                    dynamic_sql["blockId"] = block["id"]
                    break
                elif dynamic_sql["constructionPattern"] == "EXEC_STATEMENT" and ("EXEC " in block["codeText"].upper() or "EXECUTE " in block["codeText"].upper()):
                    dynamic_sql["blockId"] = block["id"]
                    break
                elif dynamic_sql["constructionPattern"] == "STRING_BUILDING":
                    # Check for string concatenation or SQL assignment patterns
                    for param in dynamic_sql["parameters"]:
                        param_pattern = re.escape(param["name"])
                        if re.search(param_pattern + r'\s*=', block["codeText"], re.IGNORECASE) or re.search(param_pattern + r'\s*\+=', block["codeText"], re.IGNORECASE):
                            dynamic_sql["blockId"] = block["id"]
                            break