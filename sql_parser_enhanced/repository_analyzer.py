"""
Repository Analyzer

Analyzes SQL stored procedures to identify repository pattern boundaries and candidates.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any, Set

logger = logging.getLogger(__name__)


class RepositoryAnalyzer:
    """
    Analyzes SQL stored procedures for repository pattern migration.
    """
    
    def __init__(self, sql_content: str, logical_blocks: List[Dict[str, Any]],
                table_references: List[Dict[str, Any]], statement_purposes: List[Dict[str, Any]],
                data_flows: List[Dict[str, Any]], parameter_usage: List[Dict[str, Any]]):
        """
        Initialize with parsing results.
        
        Args:
            sql_content: The SQL stored procedure code
            logical_blocks: Identified logical blocks
            table_references: Detected table references
            statement_purposes: Statement purpose classifications
            data_flows: Data flow analysis
            parameter_usage: Parameter usage data
        """
        self.sql_content = sql_content
        self.logical_blocks = logical_blocks
        self.table_references = table_references
        self.statement_purposes = statement_purposes
        self.data_flows = data_flows
        self.parameter_usage = parameter_usage
        self.query_patterns = []
        self.repository_boundaries = []
        self.implementation_complexity = []
    
    def detect_query_patterns(self):
        """
        Detect common query patterns in the procedure.
        
        Returns:
            List of query patterns.
        """
        # Group blocks by their purpose
        purpose_blocks = {}
        for block in self.logical_blocks:
            purpose = block.get("purpose")
            if purpose:
                if purpose not in purpose_blocks:
                    purpose_blocks[purpose] = []
                purpose_blocks[purpose].append(block)
        
        # Analyze each data retrieval and transformation block
        pattern_counter = 0
        for purpose in ["DATA_RETRIEVAL", "DATA_TRANSFORMATION", "DATA_FILTERING"]:
            if purpose in purpose_blocks:
                for block in purpose_blocks[purpose]:
                    pattern = self._analyze_block_query_pattern(block)
                    if pattern:
                        pattern["patternId"] = f"pattern_{pattern_counter}"
                        pattern_counter += 1
                        self.query_patterns.append(pattern)
        
        # Analyze data flows for multi-block patterns
        for flow in self.data_flows:
            pattern = self._analyze_flow_query_pattern(flow)
            if pattern:
                pattern["patternId"] = f"pattern_{pattern_counter}"
                pattern_counter += 1
                self.query_patterns.append(pattern)
        
        return self.query_patterns
    
    def _analyze_block_query_pattern(self, block):
        """Analyze a single block for query patterns."""
        block_id = block["id"]
        code_text = block["codeText"]
        
        # Determine tables involved
        tables = set()
        for ref in self.table_references:
            if ref.get("blockId") == block_id:
                tables.add(ref["table"])
        
        # Skip if no tables are involved
        if not tables:
            return None
        
        # Determine pattern type
        pattern_type = None
        if "SELECT" in code_text.upper() and "JOIN" not in code_text.upper() and "WHERE" in code_text.upper():
            pattern_type = "FILTERED_RETRIEVAL"
        elif "SELECT" in code_text.upper() and "JOIN" in code_text.upper():
            pattern_type = "JOINED_RETRIEVAL"
        elif "SELECT" in code_text.upper() and "GROUP BY" in code_text.upper():
            pattern_type = "AGGREGATION"
        elif "INSERT" in code_text.upper() and "SELECT" in code_text.upper():
            pattern_type = "INSERT_FROM_SELECT"
        elif "UPDATE" in code_text.upper():
            pattern_type = "UPDATE_OPERATION"
        elif "DELETE" in code_text.upper():
            pattern_type = "DELETE_OPERATION"
        else:
            pattern_type = "OTHER_OPERATION"
        
        # Skip if no pattern type identified
        if not pattern_type:
            return None
        
        # Determine complexity
        complexity = "LOW"
        if len(tables) > 2:
            complexity = "MEDIUM"
        if "CASE" in code_text.upper() or "PIVOT" in code_text.upper() or code_text.count("JOIN") > 2:
            complexity = "HIGH"
        
        # Detect join types
        join_types = []
        if "INNER JOIN" in code_text.upper():
            join_types.append("INNER")
        if "LEFT JOIN" in code_text.upper() or "LEFT OUTER JOIN" in code_text.upper():
            join_types.append("LEFT")
        if "RIGHT JOIN" in code_text.upper() or "RIGHT OUTER JOIN" in code_text.upper():
            join_types.append("RIGHT")
        if "FULL JOIN" in code_text.upper() or "FULL OUTER JOIN" in code_text.upper():
            join_types.append("FULL")
        if "CROSS JOIN" in code_text.upper():
            join_types.append("CROSS")
        
        # Detect filter types
        filter_types = []
        if re.search(r'=\s*@', code_text):
            filter_types.append("EQUALITY")
        if re.search(r'(>|<|>=|<=)\s*@', code_text):
            filter_types.append("RANGE")
        if "LIKE" in code_text.upper():
            filter_types.append("PATTERN")
        if "IN" in code_text.upper():
            filter_types.append("LIST")
        if "IS NULL" in code_text.upper() or "IS NOT NULL" in code_text.upper():
            filter_types.append("NULL_CHECK")
        
        # Assess repository compatibility
        repository_compatibility = "STANDARD"
        if "EXEC" in code_text.upper() or "EXECUTE" in code_text.upper() or "+=" in code_text:
            repository_compatibility = "COMPLEX"
        if "WITH" in code_text.upper() and "CTE" in code_text.upper():
            repository_compatibility = "ADVANCED"
        
        # Determine primary entity
        primary_entity = None
        if tables:
            # Use the table that appears most in the code or the first table
            table_counts = {table: code_text.upper().count(table.upper()) for table in tables}
            if table_counts:
                primary_entity = max(table_counts.items(), key=lambda x: x[1])[0]
            else:
                primary_entity = list(tables)[0]
        
        # Create pattern entry
        pattern = {
            "patternType": pattern_type,
            "complexity": complexity,
            "tableCount": len(tables),
            "joinTypes": join_types,
            "filterTypes": filter_types,
            "primaryEntity": primary_entity,
            "relatedEntities": list(tables - {primary_entity}) if primary_entity else list(tables),
            "repositoryCompatibility": repository_compatibility,
            "blockIds": [block_id]
        }
        
        return pattern
    
    def _analyze_flow_query_pattern(self, flow):
        """Analyze a data flow for query patterns."""
        source_entities = flow.get("sourceEntities", [])
        target_entities = flow.get("targetEntities", [])
        operations = flow.get("operations", [])
        block_ids = flow.get("blockIds", [])
        
        # Skip flows without sources or targets
        if not source_entities or not target_entities:
            return None
        
        # Determine pattern type based on the flow
        pattern_type = None
        if "AGGREGATE" in operations:
            pattern_type = "DATA_AGGREGATION"
        elif "JOIN" in operations and "FILTER" in operations:
            pattern_type = "FILTERED_JOIN"
        elif "JOIN" in operations:
            pattern_type = "DATA_JOIN"
        elif "FILTER" in operations:
            pattern_type = "DATA_FILTERING"
        elif "UPDATE" in operations:
            pattern_type = "DATA_UPDATE"
        elif "INSERT" in operations:
            pattern_type = "DATA_INSERT"
        else:
            pattern_type = "DATA_TRANSFORMATION"
        
        # Determine complexity
        complexity = "LOW"
        if len(source_entities) > 1 or len(target_entities) > 1:
            complexity = "MEDIUM"
        if len(operations) > 3:
            complexity = "HIGH"
        
        # Assess repository compatibility
        repository_compatibility = "STANDARD"
        
        # Check for any complex operations in the blocks
        for block_id in block_ids:
            block = next((b for b in self.logical_blocks if b["id"] == block_id), None)
            if block:
                code_text = block["codeText"]
                if "EXEC" in code_text.upper() or "EXECUTE" in code_text.upper() or "+=" in code_text:
                    repository_compatibility = "COMPLEX"
                if "WITH" in code_text.upper() and "CTE" in code_text.upper():
                    repository_compatibility = "ADVANCED"
        
        # Create pattern entry
        pattern = {
            "patternType": pattern_type,
            "complexity": complexity,
            "tableCount": len(source_entities) + len(target_entities),
            "joinTypes": [],  # Would need more detailed analysis
            "filterTypes": [],  # Would need more detailed analysis
            "primaryEntity": source_entities[0] if source_entities else None,
            "relatedEntities": source_entities[1:] + target_entities,
            "repositoryCompatibility": repository_compatibility,
            "blockIds": block_ids
        }
        
        return pattern
    
    def suggest_repository_boundaries(self):
        """
        Suggest logical boundaries for repository methods.
        
        Returns:
            List of repository method boundary suggestions.
        """
        # Find potential repository methods
        method_counter = 0
        
        # Group statements by their target entities
        entity_statements = {}
        for statement in self.statement_purposes:
            for entity in statement.get("affectedEntities", []):
                # Skip variable entities
                if entity.startswith('@'):
                    continue
                
                if entity not in entity_statements:
                    entity_statements[entity] = []
                entity_statements[entity].append(statement)
        
        # Create repository methods based on entity groups
        for entity, statements in entity_statements.items():
            # Skip entities with only one statement
            if len(statements) <= 1:
                continue
            
            # Check if we have RETRIEVAL statements
            retrieval_statements = [s for s in statements if s["purpose"] == "RETRIEVAL"]
            if retrieval_statements:
                method = self._create_retrieval_method(entity, retrieval_statements, method_counter)
                if method:
                    self.repository_boundaries.append(method)
                    method_counter += 1
            
            # Check if we have PERSISTENCE statements
            persistence_statements = [s for s in statements if s["purpose"] == "PERSISTENCE"]
            if persistence_statements:
                method = self._create_persistence_method(entity, persistence_statements, method_counter)
                if method:
                    self.repository_boundaries.append(method)
                    method_counter += 1
        
        # Create methods based on data flows
        for flow in self.data_flows:
            method = self._create_flow_method(flow, method_counter)
            if method:
                self.repository_boundaries.append(method)
                method_counter += 1
        
        # Create methods based on logical blocks with clear purpose
        for block in self.logical_blocks:
            # Skip blocks that are already covered by other methods
            if any(block["id"] in method.get("relatedBlocks", []) for method in self.repository_boundaries):
                continue
            
            purpose = block.get("purpose")
            if purpose in ["DATA_RETRIEVAL", "DATA_TRANSFORMATION", "DATA_FILTERING"]:
                method = self._create_block_method(block, method_counter)
                if method:
                    self.repository_boundaries.append(method)
                    method_counter += 1
        
        return self.repository_boundaries
    
    def _create_retrieval_method(self, entity, statements, method_counter):
        """Create a repository method for retrieving data."""
        # Find related blocks
        block_ids = list(set(statement.get("blockId") for statement in statements if statement.get("blockId")))
        
        # Skip if no blocks found
        if not block_ids:
            return None
        
        # Extract subpurposes for a more specific method description
        subpurposes = list(set(statement.get("subPurpose") for statement in statements if statement.get("subPurpose")))
        
        # Determine method name based on entity and subpurposes
        method_name = f"Get{entity}"
        if "ID_LOOKUP" in subpurposes:
            method_name = f"Get{entity}ById"
        elif "DATE_RANGE_FILTER" in subpurposes:
            method_name = f"Get{entity}ByDateRange"
        elif "STATUS_CHECK" in subpurposes:
            method_name = f"Get{entity}ByStatus"
        
        # Find input parameters
        input_parameters = self._find_input_parameters(block_ids)
        
        # Determine return structure
        return_structure = self._determine_return_structure(entity, statements)
        
        # Create method boundary
        method = {
            "methodId": f"method_{method_counter}",
            "suggestedName": method_name,
            "description": f"Retrieve {entity} data based on filters",
            "relatedBlocks": block_ids,
            "inputParameters": input_parameters,
            "returnDataStructure": return_structure
        }
        
        return method
    
    def _create_persistence_method(self, entity, statements, method_counter):
        """Create a repository method for persisting data."""
        # Find related blocks
        block_ids = list(set(statement.get("blockId") for statement in statements if statement.get("blockId")))
        
        # Skip if no blocks found
        if not block_ids:
            return None
        
        # Extract subpurposes for a more specific method description
        subpurposes = list(set(statement.get("subPurpose") for statement in statements if statement.get("subPurpose")))
        
        # Determine method name based on entity and subpurposes
        method_name = f"Save{entity}"
        if all(subpurpose == "SINGLE_INSERT" for subpurpose in subpurposes if subpurpose):
            method_name = f"Create{entity}"
        elif all(subpurpose == "UPDATE" for subpurpose in subpurposes if subpurpose):
            method_name = f"Update{entity}"
        elif all(subpurpose == "DELETE" for subpurpose in subpurposes if subpurpose):
            method_name = f"Delete{entity}"
        elif "LOGGING" in subpurposes:
            method_name = f"Log{entity}"
        
        # Find input parameters
        input_parameters = self._find_input_parameters(block_ids)
        
        # Create method boundary
        method = {
            "methodId": f"method_{method_counter}",
            "suggestedName": method_name,
            "description": f"Persist {entity} data",
            "relatedBlocks": block_ids,
            "inputParameters": input_parameters,
            "returnDataStructure": {
                "primaryEntity": entity,
                "returnType": "BOOL" if any(subpurpose == "DELETE" for subpurpose in subpurposes if subpurpose) else "VOID"
            }
        }
        
        return method
    
    def _create_flow_method(self, flow, method_counter):
        """Create a repository method based on a data flow."""
        source_entities = flow.get("sourceEntities", [])
        target_entities = flow.get("targetEntities", [])
        operations = flow.get("operations", [])
        block_ids = flow.get("blockIds", [])
        
        # Skip flows without sources, targets, or blocks
        if not source_entities or not target_entities or not block_ids:
            return None
        
        # Skip flows that are already covered by other methods
        if any(any(block_id in method.get("relatedBlocks", []) for block_id in block_ids) for method in self.repository_boundaries):
            return None
        
        # Determine method name based on source and target entities
        source_name = source_entities[0] if source_entities else "Source"
        target_name = target_entities[0] if target_entities else "Target"
        
        method_name = f"Process{source_name}To{target_name}"
        if "AGGREGATE" in operations:
            method_name = f"Aggregate{source_name}"
        elif "JOIN" in operations:
            method_name = f"Join{source_name}With{source_name}" if len(source_entities) > 1 else f"Process{source_name}"
        elif "FILTER" in operations:
            method_name = f"Filter{source_name}"
        elif "UPDATE" in operations:
            method_name = f"Update{target_name}From{source_name}"
        elif "INSERT" in operations:
            method_name = f"Create{target_name}From{source_name}"
        
        # Find input parameters
        input_parameters = self._find_input_parameters(block_ids)
        
        # Determine return structure
        return_structure = {
            "primaryEntity": target_entities[0] if target_entities else None,
            "sourceEntities": source_entities,
            "operations": operations
        }
        
        # Create method boundary
        method = {
            "methodId": f"method_{method_counter}",
            "suggestedName": method_name,
            "description": f"Process data from {', '.join(source_entities)} to {', '.join(target_entities)}",
            "relatedBlocks": block_ids,
            "inputParameters": input_parameters,
            "returnDataStructure": return_structure
        }
        
        return method
    
    def _create_block_method(self, block, method_counter):
        """Create a repository method based on a logical block."""
        block_id = block["id"]
        purpose = block.get("purpose")
        code_text = block["codeText"]
        
        # Skip blocks without a clear data-related purpose
        if purpose not in ["DATA_RETRIEVAL", "DATA_TRANSFORMATION", "DATA_FILTERING"]:
            return None
        
        # Find tables involved
        tables = set()
        for ref in self.table_references:
            if ref.get("blockId") == block_id:
                tables.add(ref["table"])
        
        # Skip if no tables are involved
        if not tables:
            return None
        
        # Determine primary entity
        primary_entity = None
        if tables:
            # Use the table that appears most in the code or the first table
            table_counts = {table: code_text.upper().count(table.upper()) for table in tables}
            if table_counts:
                primary_entity = max(table_counts.items(), key=lambda x: x[1])[0]
            else:
                primary_entity = list(tables)[0]
        
        # Determine method name based on purpose and primary entity
        method_name = f"Process{primary_entity}"
        if purpose == "DATA_RETRIEVAL":
            method_name = f"Get{primary_entity}"
            if "WHERE" in code_text.upper():
                method_name = f"Get{primary_entity}ByFilter"
            if "JOIN" in code_text.upper():
                method_name = f"Get{primary_entity}WithRelated"
        elif purpose == "DATA_FILTERING":
            method_name = f"Filter{primary_entity}"
        elif purpose == "DATA_TRANSFORMATION":
            method_name = f"Transform{primary_entity}"
        
        # Find input parameters
        input_parameters = self._find_input_parameters([block_id])
        
        # Determine return structure
        return_structure = {
            "primaryEntity": primary_entity,
            "includedColumns": [],  # Would need more detailed analysis
            "relatedEntities": list(tables - {primary_entity}) if primary_entity else list(tables)
        }
        
        # Create method boundary
        method = {
            "methodId": f"method_{method_counter}",
            "suggestedName": method_name,
            "description": f"{purpose.replace('_', ' ').title()} for {primary_entity}",
            "relatedBlocks": [block_id],
            "inputParameters": input_parameters,
            "returnDataStructure": return_structure
        }
        
        return method
    
    def _find_input_parameters(self, block_ids):
        """Find input parameters used in the specified blocks."""
        input_parameters = []
        
        # Check all parameter usages
        for param_data in self.parameter_usage:
            param_name = param_data["parameterName"]
            param_type = param_data["parameterType"]
            
            # Check if this parameter is used in any of the specified blocks
            used_in_blocks = False
            usage_type = None
            for occurrence in param_data["occurrences"]:
                if occurrence["blockId"] in block_ids:
                    used_in_blocks = True
                    usage_type = occurrence["usage"]
                    break
            
            if used_in_blocks:
                input_param = {
                    "name": param_name,
                    "type": param_type,
                    "usage": usage_type
                }
                input_parameters.append(input_param)
        
        return input_parameters
    
    def _determine_return_structure(self, entity, statements):
        """Determine the return data structure for a repository method."""
        # Look for columns in SELECT statements
        columns = set()
        for statement in statements:
            if statement["purpose"] == "RETRIEVAL":
                for block_id in [statement.get("blockId")]:
                    if block_id:
                        for ref in self.table_references:
                            if ref.get("blockId") == block_id and ref["table"] == entity:
                                columns.update(ref.get("columns", []))
        
        # If no specific columns found, return the entity
        if not columns:
            return {
                "primaryEntity": entity,
                "includedColumns": ["*"],
                "returnType": "ENTITY_LIST"
            }
        
        return {
            "primaryEntity": entity,
            "includedColumns": list(columns),
            "returnType": "ENTITY_LIST"
        }
    
    def analyze_implementation_complexity(self):
        """
        Analyze implementation complexity for repository migration.
        
        Returns:
            List of implementation complexity indicators.
        """
        complexity_counter = 0
        
        # Check for dynamic SQL
        self._analyze_dynamic_sql_complexity(complexity_counter)
        complexity_counter += len(self.implementation_complexity)
        
        # Check for temporary tables
        self._analyze_temp_table_complexity(complexity_counter)
        complexity_counter += len(self.implementation_complexity) - complexity_counter
        
        # Check for complex operations
        self._analyze_operation_complexity(complexity_counter)
        
        return self.implementation_complexity
    
    def _analyze_dynamic_sql_complexity(self, counter_start):
        """Analyze complexity due to dynamic SQL usage."""
        # Look for dynamic SQL in the code
        dynamic_sql_pattern = r'EXEC(?:UTE)?\s+(?:sp_executesql\s+)?[@N]'
        string_building_pattern = r'@\w+\s*\+='
        
        dynamic_sql_blocks = set()
        
        # Check for dynamic SQL patterns in blocks
        for block in self.logical_blocks:
            code_text = block["codeText"]
            if (re.search(dynamic_sql_pattern, code_text, re.IGNORECASE) or
                re.search(string_building_pattern, code_text, re.IGNORECASE)):
                dynamic_sql_blocks.add(block["id"])
        
        # Create complexity indicators for dynamic SQL
        for i, block_id in enumerate(dynamic_sql_blocks):
            block = next((b for b in self.logical_blocks if b["id"] == block_id), None)
            if block:
                # Determine complexity level
                complexity_level = "HIGH"
                reason = "Dynamic SQL construction"
                
                # Suggest migration approach
                approach = "Use IQueryable with expression building"
                if "sp_executesql" in block["codeText"].lower():
                    approach = "Use parameterized queries with a custom SQL builder"
                
                # Create complexity indicator
                complexity = {
                    "complexityId": f"complex_{counter_start + i}",
                    "blockIds": [block_id],
                    "complexityLevel": complexity_level,
                    "complexityType": "DYNAMIC_SQL",
                    "description": reason,
                    "migrationApproach": approach,
                    "alternativeApproaches": [
                        "Use stored procedure with Dapper",
                        "Implement custom SQL builder",
                        "Break complex logic into smaller repository methods"
                    ]
                }
                
                self.implementation_complexity.append(complexity)
    
    def _analyze_temp_table_complexity(self, counter_start):
        """Analyze complexity due to temporary table usage."""
        # Look for temp table creation
        temp_table_pattern = r'(CREATE|DECLARE)\s+.*?TABLE\s+[#@]'
        
        temp_table_blocks = set()
        
        # Check for temp table patterns in blocks
        for block in self.logical_blocks:
            code_text = block["codeText"]
            if re.search(temp_table_pattern, code_text, re.IGNORECASE):
                temp_table_blocks.add(block["id"])
        
        # Create complexity indicators for temp tables
        for i, block_id in enumerate(temp_table_blocks):
            block = next((b for b in self.logical_blocks if b["id"] == block_id), None)
            if block:
                # Determine complexity level
                complexity_level = "MEDIUM"
                reason = "Temporary table usage"
                
                # Temporary tables with multiple transformations are higher complexity
                if (re.search(r'UPDATE\s+[#@]', block["codeText"], re.IGNORECASE) or
                    re.search(r'DELETE\s+FROM\s+[#@]', block["codeText"], re.IGNORECASE)):
                    complexity_level = "HIGH"
                    reason = "Temporary table with transformations"
                
                # Suggest migration approach
                approach = "Use in-memory collections with LINQ"
                if complexity_level == "HIGH":
                    approach = "Use multiple repository methods with intermediate results"
                
                # Create complexity indicator
                complexity = {
                    "complexityId": f"complex_{counter_start + i}",
                    "blockIds": [block_id],
                    "complexityLevel": complexity_level,
                    "complexityType": "TEMP_TABLE",
                    "description": reason,
                    "migrationApproach": approach,
                    "alternativeApproaches": [
                        "Use stored procedure for this operation",
                        "Implement as a series of sequential database calls",
                        "Create a view for common intermediate results"
                    ]
                }
                
                self.implementation_complexity.append(complexity)
    
    def _analyze_operation_complexity(self, counter_start):
        """Analyze complexity due to complex operations."""
        complexity_counter = counter_start
        
        # Check for complex patterns
        complex_patterns = [
            (r'PIVOT', "PIVOT", "HIGH", "Complex data transformation"),
            (r'UNPIVOT', "UNPIVOT", "HIGH", "Complex data transformation"),
            (r'MERGE\s+INTO', "MERGE", "HIGH", "Complex data operation"),
            (r'CURSOR\s+FOR', "CURSOR", "HIGH", "Cursor-based processing"),
            (r'OUTPUT\s+INTO', "OUTPUT", "MEDIUM", "Output clause"),
            (r'WITH\s+(\w+)', "CTE", "MEDIUM", "Common Table Expression"),
            (r'CROSS\s+APPLY', "APPLY", "MEDIUM", "APPLY operator"),
            (r'OUTER\s+APPLY', "APPLY", "MEDIUM", "APPLY operator")
        ]
        
        # Check for complex patterns in blocks
        for pattern, pattern_type, level, description in complex_patterns:
            for block in self.logical_blocks:
                # Skip blocks already identified as complex
                if any(block["id"] in comp.get("blockIds", []) for comp in self.implementation_complexity):
                    continue
                
                code_text = block["codeText"]
                if re.search(pattern, code_text, re.IGNORECASE):
                    # Suggest migration approach
                    approach = "Use multiple repository methods"
                    if pattern_type == "CURSOR":
                        approach = "Use LINQ or collection processing instead of cursors"
                    elif pattern_type == "CTE":
                        approach = "Break into multiple repository methods or use a custom query"
                    elif pattern_type in ["PIVOT", "UNPIVOT"]:
                        approach = "Transform data in application code after retrieval"
                    
                    # Create complexity indicator
                    complexity = {
                        "complexityId": f"complex_{complexity_counter}",
                        "blockIds": [block["id"]],
                        "complexityLevel": level,
                        "complexityType": pattern_type,
                        "description": description,
                        "migrationApproach": approach,
                        "alternativeApproaches": [
                            "Use stored procedure for this operation",
                            "Implement as a series of sequential database calls",
                            "Break complex logic into smaller repository methods"
                        ]
                    }
                    
                    self.implementation_complexity.append(complexity)
                    complexity_counter += 1
                    
                    # Once found, break to avoid duplicate identification
                    break