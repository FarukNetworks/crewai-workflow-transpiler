"""
Parameter Tracker

Tracks parameter usage throughout a SQL stored procedure.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any, Set

logger = logging.getLogger(__name__)


class ParameterTracker:
    """
    Tracks parameter usage throughout a stored procedure.
    """
    
    def __init__(self, sql_content: str, logical_blocks: List[Dict[str, Any]], 
                procedure_metadata: Dict[str, Any], statement_purposes=None):
        """
        Initialize with parsing results.
        
        Args:
            sql_content: The SQL stored procedure code
            logical_blocks: Identified logical blocks
            procedure_metadata: Procedure metadata including parameters
            statement_purposes: Statement purpose classifications (optional)
        """
        self.sql_content = sql_content
        self.logical_blocks = logical_blocks
        self.procedure_metadata = procedure_metadata
        self.statement_purposes = statement_purposes or []
        self.parameter_usage = []
        self.test_value_candidates = []
    
    def analyze_parameter_usage(self):
        """
        Analyze how parameters are used throughout the procedure.
        
        Returns:
            List of parameter usage data.
        """
        # Get list of parameters from procedure metadata
        parameters = self.procedure_metadata.get("parameters", [])
        
        # Track usage for each parameter
        for param in parameters:
            param_name = param["name"]
            param_type = param.get("dataType", "")
            
            # Initialize usage data
            usage_data = {
                "parameterName": param_name,
                "parameterType": param_type,
                "defaultValue": param.get("defaultValue"),
                "usagePattern": None,
                "occurrences": []
            }
            
            # Find all occurrences of the parameter
            self._find_parameter_occurrences(param_name, usage_data)
            
            # Determine usage pattern
            usage_data["usagePattern"] = self._determine_usage_pattern(usage_data)
            
            # Add to parameter usage list
            self.parameter_usage.append(usage_data)
        
        return self.parameter_usage
    
    def _find_parameter_occurrences(self, param_name, usage_data):
        """Find all occurrences of a parameter in the SQL code."""
        # Escape special characters in parameter name for regex
        escaped_param = re.escape(param_name)
        
        # Find all occurrences
        for match in re.finditer(r'\b' + escaped_param + r'\b', self.sql_content, re.IGNORECASE):
            # Get the context around the parameter (for usage classification)
            start_pos = max(0, match.start() - 50)
            end_pos = min(len(self.sql_content), match.end() + 50)
            context = self.sql_content[start_pos:end_pos]
            
            # Get line number
            line_number = self.sql_content[:match.start()].count('\n') + 1
            
            # Find containing block
            block_id = self._find_containing_block(line_number)
            
            # Determine usage type
            usage_type = self._classify_parameter_usage(param_name, context, block_id)
            
            # Find affected entity (table/column)
            entity = self._find_affected_entity(param_name, context, block_id)
            
            # Extract condition if applicable
            condition = self._extract_condition(param_name, context)
            
            # Create occurrence entry
            occurrence = {
                "blockId": block_id,
                "lineNumber": line_number,
                "usage": usage_type,
                "context": context,
                "entity": entity,
                "condition": condition
            }
            
            usage_data["occurrences"].append(occurrence)
    
    def _find_containing_block(self, line_number):
        """Find the block containing the given line number."""
        containing_blocks = []
        
        for block in self.logical_blocks:
            if (block["lineRange"][0] <= line_number and 
                block["lineRange"][1] >= line_number):
                containing_blocks.append(block)
        
        # Return the smallest (most specific) containing block
        if containing_blocks:
            return min(containing_blocks, 
                      key=lambda b: b["lineRange"][1] - b["lineRange"][0])["id"]
        return None
    
    def _classify_parameter_usage(self, param_name, context, block_id):
        """Classify how a parameter is used based on context."""
        context_upper = context.upper()
        
        # Check for common usage patterns
        if "WHERE" in context_upper and param_name in context_upper:
            if re.search(r'WHERE\b.+?' + re.escape(param_name) + r'.+?=', context_upper, re.IGNORECASE):
                return "FILTER_CONDITION"
            elif re.search(r'WHERE\b.+?' + re.escape(param_name) + r'.+?LIKE', context_upper, re.IGNORECASE):
                return "FILTER_PATTERN"
            elif re.search(r'WHERE\b.+?' + re.escape(param_name) + r'.+?IN', context_upper, re.IGNORECASE):
                return "FILTER_LIST"
            elif re.search(r'WHERE\b.+?' + re.escape(param_name) + r'.+?(>|<|>=|<=)', context_upper, re.IGNORECASE):
                return "FILTER_RANGE"
            else:
                return "FILTER_OTHER"
        
        elif "JOIN" in context_upper and param_name in context_upper:
            if re.search(r'JOIN\b.+?ON\b.+?' + re.escape(param_name), context_upper, re.IGNORECASE):
                return "JOIN_CONDITION"
            else:
                return "JOIN_OTHER"
        
        elif "ORDER BY" in context_upper and param_name in context_upper:
            return "SORT_PARAMETER"
        
        elif "GROUP BY" in context_upper and param_name in context_upper:
            return "GROUP_PARAMETER"
        
        elif "HAVING" in context_upper and param_name in context_upper:
            return "HAVING_CONDITION"
        
        elif "SELECT" in context_upper and "TOP" in context_upper and param_name in context_upper:
            return "PAGINATION_LIMIT"
        
        elif "OFFSET" in context_upper and param_name in context_upper:
            return "PAGINATION_OFFSET"
        
        elif "INSERT" in context_upper and "VALUES" in context_upper and param_name in context_upper:
            return "INSERT_VALUE"
        
        elif "UPDATE" in context_upper and "SET" in context_upper and param_name in context_upper:
            return "UPDATE_VALUE"
        
        elif "IF" in context_upper and param_name in context_upper:
            return "CONDITIONAL_CHECK"
        
        elif "RETURN" in context_upper and param_name in context_upper:
            return "RETURN_VALUE"
        
        elif "EXEC" in context_upper and param_name in context_upper:
            return "PROCEDURE_PARAMETER"
        
        elif "DECLARE" in context_upper and "=" in context_upper and param_name in context_upper:
            return "VARIABLE_INITIALIZATION"
        
        elif "SET" in context_upper and "=" in context_upper and param_name in context_upper:
            return "VARIABLE_ASSIGNMENT"
        
        # Check if this is part of a dynamic SQL construction
        elif "SP_EXECUTESQL" in context_upper or "+=" in context_upper or "CONCAT" in context_upper:
            return "DYNAMIC_SQL_PARAMETER"
        
        # If we get here, it's some other usage
        return "OTHER_USAGE"
    
    def _find_affected_entity(self, param_name, context, block_id):
        """Determine which entity (table/column) is affected by this parameter."""
        # Look for table/column names in the context
        tables_in_context = set()
        
        # Check the block's table references
        for block in self.logical_blocks:
            if block["id"] == block_id:
                # Get table references for this block
                for statement in self.statement_purposes:
                    if statement.get("blockId") == block_id:
                        for entity in statement.get("affectedEntities", []):
                            if not entity.startswith('@'):  # Skip variables
                                tables_in_context.add(entity)
        
        # If we have only one table, it's likely the affected entity
        if len(tables_in_context) == 1:
            return list(tables_in_context)[0]
        
        # If we have multiple tables, try to determine the most relevant one
        elif len(tables_in_context) > 1:
            # Look for usage with specific columns
            for table in tables_in_context:
                column_pattern = r'\b' + re.escape(table) + r'\.(\w+)\b.+?\b' + re.escape(param_name) + r'\b'
                column_match = re.search(column_pattern, context, re.IGNORECASE)
                if column_match:
                    column_name = column_match.group(1)
                    return f"{table}.{column_name}"
                
                # Check if the parameter is used in a WHERE clause with this table
                where_pattern = r'WHERE\b.+?\b' + re.escape(table) + r'\b.+?\b' + re.escape(param_name) + r'\b'
                if re.search(where_pattern, context, re.IGNORECASE):
                    return table
        
        # If we can't determine a specific entity, return None
        return None
    
    def _extract_condition(self, param_name, context):
        """Extract the condition in which the parameter is used."""
        # Look for conditions in WHERE clauses
        where_pattern = r'WHERE\b(.+?)(?:ORDER BY|GROUP BY|HAVING|;|$)'
        where_match = re.search(where_pattern, context, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1).strip()
            # Find the specific condition involving the parameter
            param_conditions = []
            conditions = re.split(r'\bAND\b|\bOR\b', where_clause, flags=re.IGNORECASE)
            for cond in conditions:
                if param_name in cond:
                    param_conditions.append(cond.strip())
            
            if param_conditions:
                return " AND ".join(param_conditions)
        
        # Look for conditions in IF statements
        if_pattern = r'IF\b(.+?)(?:BEGIN|THEN|\n)'
        if_match = re.search(if_pattern, context, re.IGNORECASE | re.DOTALL)
        if if_match:
            if_clause = if_match.group(1).strip()
            # Find the specific condition involving the parameter
            param_conditions = []
            conditions = re.split(r'\bAND\b|\bOR\b', if_clause, flags=re.IGNORECASE)
            for cond in conditions:
                if param_name in cond:
                    param_conditions.append(cond.strip())
            
            if param_conditions:
                return " AND ".join(param_conditions)
        
        # Look for conditions in JOIN clauses
        join_pattern = r'JOIN\b.+?ON\b(.+?)(?:JOIN|WHERE|GROUP BY|ORDER BY|;|$)'
        join_match = re.search(join_pattern, context, re.IGNORECASE | re.DOTALL)
        if join_match:
            join_clause = join_match.group(1).strip()
            # Find the specific condition involving the parameter
            param_conditions = []
            conditions = re.split(r'\bAND\b|\bOR\b', join_clause, flags=re.IGNORECASE)
            for cond in conditions:
                if param_name in cond:
                    param_conditions.append(cond.strip())
            
            if param_conditions:
                return " AND ".join(param_conditions)
        
        # If we can't extract a specific condition, return None
        return None
    
    def _determine_usage_pattern(self, usage_data):
        """Determine the overall usage pattern for a parameter."""
        # Count occurrence types
        usage_counts = {}
        for occurrence in usage_data["occurrences"]:
            usage_type = occurrence["usage"]
            usage_counts[usage_type] = usage_counts.get(usage_type, 0) + 1
        
        # Get the most common usage type
        if usage_counts:
            most_common_usage = max(usage_counts.items(), key=lambda x: x[1])[0]
            
            # Determine high-level pattern
            if most_common_usage.startswith("FILTER_"):
                return "FILTER_PARAMETER"
            elif most_common_usage.startswith("JOIN_"):
                return "JOIN_PARAMETER"
            elif most_common_usage.startswith("PAGINATION_"):
                return "PAGINATION_PARAMETER"
            elif most_common_usage in ["INSERT_VALUE", "UPDATE_VALUE"]:
                return "DATA_VALUE"
            elif most_common_usage == "CONDITIONAL_CHECK":
                return "CONTROL_PARAMETER"
            elif most_common_usage == "DYNAMIC_SQL_PARAMETER":
                return "DYNAMIC_SQL_PARAMETER"
            else:
                return most_common_usage
        
        return "UNUSED_PARAMETER"
    
    def extract_test_values(self):
        """
        Extract potential test values for parameters based on their usage.
        
        Returns:
            List of test value candidates.
        """
        test_values = []
        
        for param_data in self.parameter_usage:
            param_name = param_data["parameterName"]
            param_type = param_data["parameterType"]
            default_value = param_data["defaultValue"]
            usage_pattern = param_data["usagePattern"]
            
            # Initialize test value entry
            test_value_entry = {
                "parameterName": param_name,
                "dataType": param_type,
                "defaultValue": default_value,
                "usageContext": usage_pattern,
                "conditions": [],
                "suggestedTestValues": []
            }
            
            # Extract conditions from occurrences
            for occurrence in param_data["occurrences"]:
                if occurrence["condition"] and occurrence["condition"] not in test_value_entry["conditions"]:
                    test_value_entry["conditions"].append({
                        "condition": occurrence["condition"],
                        "entity": occurrence["entity"],
                        "usage": occurrence["usage"]
                    })
            
            # Generate suggested test values based on parameter type and usage
            suggested_values = self._generate_test_values(param_data)
            test_value_entry["suggestedTestValues"] = suggested_values
            
            test_values.append(test_value_entry)
        
        self.test_value_candidates = test_values
        return test_values
    
    def _generate_test_values(self, param_data):
        """Generate suggested test values for a parameter."""
        param_type = param_data["parameterType"].upper()
        usage_pattern = param_data["usagePattern"]
        default_value = param_data["defaultValue"]
        
        suggested_values = []
        
        # Extract literal values from conditions
        literal_values = set()
        for occurrence in param_data["occurrences"]:
            if occurrence["condition"]:
                # Look for literals in the condition
                literals = re.findall(r'=\s*\'([^\']*)\'|\=\s*(\d+)', occurrence["condition"])
                for literal in literals:
                    for value in literal:
                        if value:
                            literal_values.add(value)
        
        # Add default value if provided
        if default_value and default_value not in ['NULL', 'null']:
            suggested_values.append({
                "value": default_value.strip("'"),
                "purpose": "DEFAULT_VALUE",
                "scenario": "Default case"
            })
        
        # Add literal values from conditions
        for value in literal_values:
            suggested_values.append({
                "value": value,
                "purpose": "LITERAL_VALUE",
                "scenario": "Value from condition"
            })
        
        # Add type-specific test values
        if "INT" in param_type or "NUMERIC" in param_type or "DECIMAL" in param_type:
            # For numeric types
            if usage_pattern == "FILTER_PARAMETER":
                suggested_values.append({
                    "value": "0",
                    "purpose": "BOUNDARY_VALUE",
                    "scenario": "Zero value"
                })
                suggested_values.append({
                    "value": "-1",
                    "purpose": "NEGATIVE_VALUE",
                    "scenario": "Negative value"
                })
                suggested_values.append({
                    "value": "2147483647",  # INT MAX
                    "purpose": "EXTREME_VALUE",
                    "scenario": "Maximum integer value"
                })
            elif usage_pattern == "PAGINATION_PARAMETER":
                suggested_values.append({
                    "value": "0",
                    "purpose": "BOUNDARY_VALUE",
                    "scenario": "Zero page/offset"
                })
                suggested_values.append({
                    "value": "1",
                    "purpose": "COMMON_VALUE",
                    "scenario": "First page"
                })
                suggested_values.append({
                    "value": "100",
                    "purpose": "COMMON_VALUE",
                    "scenario": "Large page size"
                })
        
        elif "VARCHAR" in param_type or "CHAR" in param_type or "TEXT" in param_type:
            # For string types
            if usage_pattern == "FILTER_PARAMETER":
                suggested_values.append({
                    "value": "",
                    "purpose": "BOUNDARY_VALUE",
                    "scenario": "Empty string"
                })
                suggested_values.append({
                    "value": "%",
                    "purpose": "WILDCARD_VALUE",
                    "scenario": "Wildcard (all values)"
                })
                
                # Add a string with max length if specified
                match = re.search(r'VARCHAR\((\d+)\)', param_type, re.IGNORECASE)
                if match:
                    max_length = int(match.group(1))
                    suggested_values.append({
                        "value": "X" * max_length,
                        "purpose": "BOUNDARY_VALUE",
                        "scenario": f"Maximum length ({max_length} characters)"
                    })
        
        elif "DATE" in param_type or "TIME" in param_type:
            # For date/time types
            suggested_values.append({
                "value": "GETDATE()",
                "purpose": "CURRENT_VALUE",
                "scenario": "Current date/time"
            })
            suggested_values.append({
                "value": "NULL",
                "purpose": "NULL_VALUE",
                "scenario": "Null date/time"
            })
            if usage_pattern == "FILTER_PARAMETER":
                suggested_values.append({
                    "value": "DATEADD(day, -30, GETDATE())",
                    "purpose": "RELATIVE_VALUE",
                    "scenario": "30 days ago"
                })
                suggested_values.append({
                    "value": "DATEADD(day, 30, GETDATE())",
                    "purpose": "RELATIVE_VALUE",
                    "scenario": "30 days in future"
                })
        
        # Add NULL test for all parameters
        suggested_values.append({
            "value": "NULL",
            "purpose": "NULL_VALUE",
            "scenario": "Null value handling"
        })
        
        return suggested_values
    
    def get_parameter_dependencies(self):
        """
        Identify dependencies between parameters.
        
        Returns:
            List of parameter dependencies.
        """
        dependencies = []
        
        # Look for parameters that are used together
        parameter_blocks = {}
        for param_data in self.parameter_usage:
            param_name = param_data["parameterName"]
            for occurrence in param_data["occurrences"]:
                block_id = occurrence["blockId"]
                if block_id not in parameter_blocks:
                    parameter_blocks[block_id] = set()
                parameter_blocks[block_id].add(param_name)
        
        # Find parameters that commonly appear together
        for block_id, params in parameter_blocks.items():
            if len(params) > 1:
                # Get all pairs of parameters in this block
                param_list = list(params)
                for i in range(len(param_list)):
                    for j in range(i+1, len(param_list)):
                        param1 = param_list[i]
                        param2 = param_list[j]
                        
                        # Check if these parameters are used in the same statement/condition
                        for statement in self.statement_purposes:
                            if statement.get("blockId") == block_id:
                                if param1 in statement["statementText"] and param2 in statement["statementText"]:
                                    # Determine relationship type
                                    relationship = "USED_TOGETHER"
                                    for condition in statement.get("conditions", []):
                                        if param1 in condition and param2 in condition:
                                            relationship = "CONDITIONAL_RELATIONSHIP"
                                    
                                    dependency = {
                                        "parameter1": param1,
                                        "parameter2": param2,
                                        "relationship": relationship,
                                        "blockId": block_id,
                                        "statementId": statement.get("statementId")
                                    }
                                    
                                    # Check if this dependency already exists
                                    exists = False
                                    for dep in dependencies:
                                        if (dep["parameter1"] == dependency["parameter1"] and
                                            dep["parameter2"] == dependency["parameter2"] and
                                            dep["relationship"] == dependency["relationship"]):
                                            exists = True
                                            break
                                    
                                    if not exists:
                                        dependencies.append(dependency)
        
        return dependencies