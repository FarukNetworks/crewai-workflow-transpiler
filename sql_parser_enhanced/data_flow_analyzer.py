"""
Data Flow Analyzer

Analyzes data flow between entities in a SQL stored procedure.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any, Set

logger = logging.getLogger(__name__)


class DataFlowAnalyzer:
    """
    Analyzes data flow between entities in a SQL stored procedure.
    """
    
    def __init__(self, sql_content: str, logical_blocks: List[Dict[str, Any]], 
                table_references: List[Dict[str, Any]], dynamic_sql_ops=None, 
                temp_structures=None):
        """
        Initialize with parsing results.
        
        Args:
            sql_content: The SQL stored procedure code
            logical_blocks: Identified logical blocks
            table_references: Detected table references
            dynamic_sql_ops: Dynamic SQL operations (optional)
            temp_structures: Temporary table structures (optional)
        """
        self.sql_content = sql_content
        self.logical_blocks = logical_blocks
        self.table_references = table_references
        self.dynamic_sql_ops = dynamic_sql_ops or []
        self.temp_structures = temp_structures or []
        self.variable_assignments = []
        self.data_flows = []
        
        # Extract variables and their assignments
        self._extract_variable_assignments()
    
    def _extract_variable_assignments(self):
        """Extract variable assignments from the SQL code."""
        # Pattern for SET @var = value
        set_pattern = r'SET\s+(@\w+)\s*=\s*([^;]+)'
        for match in re.finditer(set_pattern, self.sql_content, re.IGNORECASE):
            var_name = match.group(1)
            value_expr = match.group(2).strip()
            
            # Find the containing block
            block_id = self._find_containing_block_id(match.start())
            
            assignment = {
                "variable": var_name,
                "expression": value_expr,
                "blockId": block_id,
                "lineNumber": self.sql_content[:match.start()].count('\n') + 1,
                "sourceEntities": self._extract_entities_from_expr(value_expr)
            }
            self.variable_assignments.append(assignment)
        
        # Pattern for SELECT @var = value
        select_pattern = r'SELECT\s+(@\w+)\s*=\s*([^,;]+)'
        for match in re.finditer(select_pattern, self.sql_content, re.IGNORECASE):
            var_name = match.group(1)
            value_expr = match.group(2).strip()
            
            # Find the containing block
            block_id = self._find_containing_block_id(match.start())
            
            assignment = {
                "variable": var_name,
                "expression": value_expr,
                "blockId": block_id,
                "lineNumber": self.sql_content[:match.start()].count('\n') + 1,
                "sourceEntities": self._extract_entities_from_expr(value_expr)
            }
            self.variable_assignments.append(assignment)
    
    def _find_containing_block_id(self, position):
        """Find the block containing the given position in the SQL content."""
        containing_blocks = []
        position_line = self.sql_content[:position].count('\n') + 1
        
        for block in self.logical_blocks:
            if (block["lineRange"][0] <= position_line and 
                block["lineRange"][1] >= position_line):
                containing_blocks.append(block)
        
        # Return the smallest (most specific) containing block
        if containing_blocks:
            return min(containing_blocks, 
                       key=lambda b: b["lineRange"][1] - b["lineRange"][0])["id"]
        return None
    
    def _extract_entities_from_expr(self, expression):
        """Extract table/entity references from an expression."""
        entities = []
        
        # Look for table names in the expression
        for table_ref in self.table_references:
            table_name = table_ref["table"]
            if re.search(r'\b' + re.escape(table_name) + r'\b', expression, re.IGNORECASE):
                entities.append(table_name)
        
        return entities
    
    def analyze_data_flows(self):
        """
        Analyze data flows between entities in the procedure.
        
        Returns:
            List of data flow mappings.
        """
        # Analyze direct table-to-table flows
        self._analyze_table_flows()
        
        # Analyze variable-mediated flows
        self._analyze_variable_flows()
        
        # Analyze flows through temp tables
        self._analyze_temp_table_flows()
        
        return self.data_flows
    
    def _analyze_table_flows(self):
        """Analyze direct flows between tables (e.g., INSERT INTO ... SELECT FROM)."""
        # Group table references by block
        block_references = {}
        for ref in self.table_references:
            block_id = ref.get("blockId")
            if block_id:
                if block_id not in block_references:
                    block_references[block_id] = []
                block_references[block_id].append(ref)
        
        # For each block, look for source → target flows
        for block_id, references in block_references.items():
            # Find INSERT/UPDATE targets
            targets = [ref for ref in references 
                      if ref["operation"] in ["INSERT", "UPDATE"]]
            
            # Find SELECT sources
            sources = [ref for ref in references 
                      if ref["operation"] == "SELECT"]
            
            # If we have both targets and sources in the same block, create a flow
            if targets and sources:
                for target in targets:
                    flow_id = f"flow_{len(self.data_flows)}"
                    
                    # Determine operations applied
                    operations = []
                    block = next((b for b in self.logical_blocks if b["id"] == block_id), None)
                    if block:
                        if "JOIN" in block["codeText"].upper():
                            operations.append("JOIN")
                        if "WHERE" in block["codeText"].upper():
                            operations.append("FILTER")
                        if "GROUP BY" in block["codeText"].upper():
                            operations.append("AGGREGATE")
                        if "ORDER BY" in block["codeText"].upper():
                            operations.append("SORT")
                    
                    if target["operation"] == "INSERT":
                        operations.append("INSERT")
                    elif target["operation"] == "UPDATE":
                        operations.append("UPDATE")
                    
                    # Create the flow
                    flow = {
                        "flowId": flow_id,
                        "sourceEntities": [s["table"] for s in sources],
                        "intermediateEntities": [],
                        "targetEntities": [target["table"]],
                        "operations": operations,
                        "blockIds": [block_id]
                    }
                    
                    # Add transformations if we can detect them
                    flow["transformations"] = self._extract_transformations(block["codeText"])
                    
                    self.data_flows.append(flow)
    
    def _analyze_variable_flows(self):
        """Analyze flows mediated by variables."""
        # Look for variables that are populated from tables and then used in table operations
        for var_assignment in self.variable_assignments:
            # Find source entities for this variable
            var_name = var_assignment["variable"]
            source_entities = var_assignment["sourceEntities"]
            
            if source_entities:
                # Look for usages of this variable in table operations
                for ref in self.table_references:
                    # Check if this variable is used in operations on this table
                    block_id = ref.get("blockId")
                    if block_id:
                        block = next((b for b in self.logical_blocks if b["id"] == block_id), None)
                        if block and var_name in block["codeText"]:
                            # This variable is used in an operation on this table
                            flow_id = f"flow_{len(self.data_flows)}"
                            
                            # Determine the operation type
                            operations = []
                            if ref["operation"] == "INSERT":
                                operations.append("INSERT")
                            elif ref["operation"] == "UPDATE":
                                operations.append("UPDATE")
                            elif ref["operation"] == "DELETE":
                                operations.append("DELETE")
                            elif "WHERE" in block["codeText"].upper():
                                operations.append("FILTER")
                            
                            # Create the flow
                            flow = {
                                "flowId": flow_id,
                                "sourceEntities": source_entities,
                                "intermediateEntities": [var_name],
                                "targetEntities": [ref["table"]],
                                "operations": operations,
                                "blockIds": [var_assignment["blockId"], block_id]
                            }
                            
                            self.data_flows.append(flow)
    
    def _analyze_temp_table_flows(self):
        """Analyze flows through temporary tables."""
        # This is handled in the existing temp_structures analysis
        # We just need to convert it to our data flow format
        for temp in self.temp_structures:
            source_entities = []
            target_entities = []
            
            # Get source entities
            for pop in temp.get("populatedFrom", []):
                if "tableReference" in pop:
                    source_entities.append(pop["tableReference"])
            
            # Get target entities
            for usage in temp.get("usedIn", []):
                if "operation" in usage and usage["operation"].startswith("INSERT_INTO_"):
                    target = usage["operation"].replace("INSERT_INTO_", "")
                    target_entities.append(target)
            
            if source_entities or target_entities:
                flow_id = f"flow_{len(self.data_flows)}"
                
                # Determine operations
                operations = []
                for transform in temp.get("transformations", []):
                    if transform["type"] not in operations:
                        operations.append(transform["type"])
                
                # Create the flow
                flow = {
                    "flowId": flow_id,
                    "sourceEntities": source_entities,
                    "intermediateEntities": [temp["name"]],
                    "targetEntities": target_entities,
                    "operations": operations,
                    "blockIds": []
                }
                
                # Collect block IDs
                for pop in temp.get("populatedFrom", []):
                    if "blockId" in pop and pop["blockId"] not in flow["blockIds"]:
                        flow["blockIds"].append(pop["blockId"])
                
                for usage in temp.get("usedIn", []):
                    if "blockId" in usage and usage["blockId"] not in flow["blockIds"]:
                        flow["blockIds"].append(usage["blockId"])
                
                self.data_flows.append(flow)
    
    def _extract_transformations(self, code_text):
        """Extract transformation details from SQL code."""
        transformations = []
        
        # Look for JOINs
        join_pattern = r'(\w+\s+)?JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.+?)(?:WHERE|GROUP BY|ORDER BY|$)'
        for match in re.finditer(join_pattern, code_text, re.IGNORECASE | re.DOTALL):
            join_type = match.group(1).strip() if match.group(1) else "INNER"
            table_name = match.group(2)
            join_alias = match.group(3) if match.group(3) else table_name
            join_condition = match.group(4).strip()
            
            transformation = {
                "type": "JOIN",
                "joinType": join_type.upper(),
                "entities": [table_name],
                "joinCondition": join_condition
            }
            transformations.append(transformation)
        
        # Look for aggregations
        agg_pattern = r'(SUM|AVG|MIN|MAX|COUNT)\s*\(([^)]+)\)'
        for match in re.finditer(agg_pattern, code_text, re.IGNORECASE):
            agg_function = match.group(1).upper()
            agg_column = match.group(2).strip()
            
            transformation = {
                "type": "AGGREGATE",
                "function": agg_function,
                "sourceColumn": agg_column
            }
            transformations.append(transformation)
        
        # Look for filtering
        filter_pattern = r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|HAVING|$)'
        for match in re.finditer(filter_pattern, code_text, re.IGNORECASE | re.DOTALL):
            filter_condition = match.group(1).strip()
            
            transformation = {
                "type": "FILTER",
                "condition": filter_condition
            }
            transformations.append(transformation)
        
        return transformations
    
    def get_entity_relationships(self):
        """Extract entity relationships from data flows."""
        relationships = []
        
        # Extract relationships from data flows
        for flow in self.data_flows:
            for source in flow.get("sourceEntities", []):
                for target in flow.get("targetEntities", []):
                    # Skip self-relationships unless it's an UPDATE
                    if source == target and "UPDATE" not in flow.get("operations", []):
                        continue
                    
                    # Determine relationship type
                    rel_type = "REFERENCES"
                    if "JOIN" in flow.get("operations", []):
                        rel_type = "JOINS"
                    elif "INSERT" in flow.get("operations", []):
                        rel_type = "POPULATES"
                    elif "UPDATE" in flow.get("operations", []):
                        rel_type = "UPDATES"
                    
                    relationship = {
                        "sourceEntity": source,
                        "targetEntity": target,
                        "relationType": rel_type,
                        "flowId": flow["flowId"],
                        "operations": flow.get("operations", [])
                    }
                    
                    # Check if this relationship already exists
                    exists = False
                    for rel in relationships:
                        if (rel["sourceEntity"] == relationship["sourceEntity"] and
                            rel["targetEntity"] == relationship["targetEntity"] and
                            rel["relationType"] == relationship["relationType"]):
                            exists = True
                            break
                    
                    if not exists:
                        relationships.append(relationship)
        
        return relationships