"""
Statement Classifier

Classifies SQL statements by their functional purpose.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any, Set

logger = logging.getLogger(__name__)


class StatementClassifier:
    """
    Classifies SQL statements by their functional purpose.
    """
    
    def __init__(self, sql_content: str, logical_blocks: List[Dict[str, Any]], 
                table_references: List[Dict[str, Any]]):
        """
        Initialize with parsing results.
        
        Args:
            sql_content: The SQL stored procedure code
            logical_blocks: Identified logical blocks
            table_references: Detected table references
        """
        self.sql_content = sql_content
        self.logical_blocks = logical_blocks
        self.table_references = table_references
        self.statements = []
        self.statement_purposes = []
        
        # Extract individual statements from the SQL code
        self._extract_statements()
    
    def _extract_statements(self):
        """Extract individual SQL statements from the stored procedure."""
        # Split SQL content by semicolons, but handle exceptions like string literals
        lines = self.sql_content.split('\n')
        current_statement = ""
        current_statement_start_line = 0
        in_string = False
        in_comment = False
        
        for i, line in enumerate(lines):
            j = 0
            while j < len(line):
                # Handle string literals
                if line[j:j+1] == "'" and not in_comment:
                    in_string = not in_string
                
                # Handle comments
                elif line[j:j+2] == '--' and not in_string:
                    in_comment = True
                elif line[j:j+2] == '/*' and not in_string:
                    in_comment = True
                elif line[j:j+2] == '*/' and in_comment:
                    in_comment = False
                    j += 1  # Skip closing */
                
                # Handle statement termination
                elif line[j:j+1] == ';' and not in_string and not in_comment:
                    # Complete the current statement
                    current_statement += line[:j+1]
                    
                    # Skip leading whitespace and empty lines
                    statement_text = current_statement.strip()
                    if statement_text and not statement_text.startswith('--'):
                        # Find the containing block
                        block_id = self._find_containing_block(current_statement_start_line, i)
                        
                        # Create statement entry
                        statement = {
                            "statementId": f"stmt_{len(self.statements)}",
                            "statementText": statement_text,
                            "lineRange": [current_statement_start_line, i],
                            "blockId": block_id
                        }
                        self.statements.append(statement)
                    
                    # Start a new statement
                    current_statement = line[j+1:]
                    current_statement_start_line = i
                    line = line[j+1:]
                    j = 0
                    continue
                
                # Reset comment flag at end of line
                if j == len(line) - 1 and in_comment and not line.endswith('*/'):
                    in_comment = False
                
                j += 1
            
            # Add the current line to the current statement
            if j >= len(line):
                current_statement += line + '\n'
        
        # Handle the last statement if it doesn't end with semicolon
        if current_statement.strip() and not current_statement.strip().startswith('--'):
            block_id = self._find_containing_block(current_statement_start_line, len(lines) - 1)
            statement = {
                "statementId": f"stmt_{len(self.statements)}",
                "statementText": current_statement.strip(),
                "lineRange": [current_statement_start_line, len(lines) - 1],
                "blockId": block_id
            }
            self.statements.append(statement)
    
    def _find_containing_block(self, start_line, end_line):
        """Find the smallest block containing the given line range."""
        containing_blocks = []
        
        for block in self.logical_blocks:
            if (block["lineRange"][0] <= start_line and 
                block["lineRange"][1] >= end_line):
                containing_blocks.append(block)
        
        # Return the smallest (most specific) containing block
        if containing_blocks:
            return min(containing_blocks, 
                      key=lambda b: b["lineRange"][1] - b["lineRange"][0])["id"]
        return None
    
    def classify_statements(self):
        """
        Classify each statement by purpose.
        
        Returns:
            List of statement classifications.
        """
        for statement in self.statements:
            # Reset purpose data
            purpose_data = {
                "statementId": statement["statementId"],
                "blockId": statement["blockId"],
                "lineRange": statement["lineRange"],
                "statementText": statement["statementText"],
                "purpose": None,
                "subPurpose": None,
                "affectedEntities": [],
                "conditions": []
            }
            
            # Classify the statement
            purpose_data = self._classify_statement_purpose(statement, purpose_data)
            
            # Extract affected entities
            purpose_data["affectedEntities"] = self._extract_affected_entities(statement)
            
            # Extract conditions
            purpose_data["conditions"] = self._extract_conditions(statement)
            
            # Add to the list of statement purposes
            self.statement_purposes.append(purpose_data)
        
        return self.statement_purposes
    
    def _classify_statement_purpose(self, statement, purpose_data):
        """Classify a statement by its purpose."""
        stmt_text = statement["statementText"].upper()
        
        # Extract the first word for easier classification
        first_word = stmt_text.split()[0] if stmt_text.split() else ""
        
        # Classification by statement type
        if first_word == "SELECT":
            if "INTO" in stmt_text and not "INSERT INTO" in stmt_text:
                purpose_data["purpose"] = "RETRIEVAL"
                if "=" in stmt_text and stmt_text.count('@') > 0:
                    purpose_data["subPurpose"] = "VARIABLE_ASSIGNMENT"
                else:
                    purpose_data["subPurpose"] = "RESULT_SET"
            else:
                purpose_data["purpose"] = "RETRIEVAL"
                if "COUNT(" in stmt_text or "SUM(" in stmt_text or "AVG(" in stmt_text:
                    purpose_data["subPurpose"] = "AGGREGATION"
                elif "JOIN" in stmt_text:
                    purpose_data["subPurpose"] = "JOIN_QUERY"
                else:
                    purpose_data["subPurpose"] = "SIMPLE_QUERY"
        
        elif first_word == "INSERT":
            purpose_data["purpose"] = "PERSISTENCE"
            if "SELECT" in stmt_text:
                purpose_data["subPurpose"] = "BULK_INSERT"
            else:
                purpose_data["subPurpose"] = "SINGLE_INSERT"
        
        elif first_word == "UPDATE":
            purpose_data["purpose"] = "PERSISTENCE"
            purpose_data["subPurpose"] = "UPDATE"
        
        elif first_word == "DELETE":
            purpose_data["purpose"] = "PERSISTENCE"
            purpose_data["subPurpose"] = "DELETE"
        
        elif first_word == "MERGE":
            purpose_data["purpose"] = "PERSISTENCE"
            purpose_data["subPurpose"] = "MERGE"
        
        elif first_word == "CREATE":
            if "TABLE" in stmt_text:
                purpose_data["purpose"] = "STRUCTURE"
                if "#" in stmt_text:
                    purpose_data["subPurpose"] = "TEMP_TABLE_CREATION"
                else:
                    purpose_data["subPurpose"] = "TABLE_CREATION"
            elif "INDEX" in stmt_text:
                purpose_data["purpose"] = "STRUCTURE"
                purpose_data["subPurpose"] = "INDEX_CREATION"
            else:
                purpose_data["purpose"] = "STRUCTURE"
                purpose_data["subPurpose"] = "OBJECT_CREATION"
        
        elif first_word == "ALTER":
            purpose_data["purpose"] = "STRUCTURE"
            purpose_data["subPurpose"] = "OBJECT_MODIFICATION"
        
        elif first_word == "DROP":
            purpose_data["purpose"] = "STRUCTURE"
            purpose_data["subPurpose"] = "OBJECT_DELETION"
        
        elif first_word == "DECLARE":
            purpose_data["purpose"] = "VARIABLE"
            if "TABLE" in stmt_text:
                purpose_data["subPurpose"] = "TABLE_VARIABLE"
            else:
                purpose_data["subPurpose"] = "VARIABLE_DECLARATION"
        
        elif first_word == "SET":
            purpose_data["purpose"] = "VARIABLE"
            if "NOCOUNT" in stmt_text or "XACT_ABORT" in stmt_text:
                purpose_data["subPurpose"] = "SESSION_SETTING"
            else:
                purpose_data["subPurpose"] = "VARIABLE_ASSIGNMENT"
        
        elif first_word == "IF":
            purpose_data["purpose"] = "CONTROL_FLOW"
            if "EXISTS" in stmt_text:
                purpose_data["subPurpose"] = "EXISTENCE_CHECK"
            elif "NULL" in stmt_text or "IS NULL" in stmt_text:
                purpose_data["subPurpose"] = "NULL_CHECK"
            elif re.search(r'@\w+\s*[<>=!]', stmt_text):
                purpose_data["subPurpose"] = "VARIABLE_CONDITION"
            else:
                purpose_data["subPurpose"] = "GENERAL_CONDITION"
        
        elif first_word == "WHILE":
            purpose_data["purpose"] = "CONTROL_FLOW"
            purpose_data["subPurpose"] = "LOOP"
        
        elif first_word == "BEGIN":
            if "TRANSACTION" in stmt_text or "TRAN" in stmt_text:
                purpose_data["purpose"] = "TRANSACTION_CONTROL"
                purpose_data["subPurpose"] = "BEGIN_TRANSACTION"
            elif "TRY" in stmt_text:
                purpose_data["purpose"] = "ERROR_HANDLING"
                purpose_data["subPurpose"] = "TRY_BLOCK"
            elif "CATCH" in stmt_text:
                purpose_data["purpose"] = "ERROR_HANDLING"
                purpose_data["subPurpose"] = "CATCH_BLOCK"
            else:
                purpose_data["purpose"] = "CONTROL_FLOW"
                purpose_data["subPurpose"] = "BLOCK_STRUCTURE"
        
        elif first_word == "COMMIT":
            purpose_data["purpose"] = "TRANSACTION_CONTROL"
            purpose_data["subPurpose"] = "COMMIT"
        
        elif first_word == "ROLLBACK":
            purpose_data["purpose"] = "TRANSACTION_CONTROL"
            purpose_data["subPurpose"] = "ROLLBACK"
        
        elif first_word == "RETURN":
            purpose_data["purpose"] = "CONTROL_FLOW"
            purpose_data["subPurpose"] = "PROCEDURE_EXIT"
        
        elif first_word == "EXEC" or first_word == "EXECUTE":
            purpose_data["purpose"] = "EXECUTION"
            if "SP_EXECUTESQL" in stmt_text:
                purpose_data["subPurpose"] = "DYNAMIC_SQL"
            else:
                purpose_data["subPurpose"] = "PROCEDURE_CALL"
        
        elif first_word == "THROW" or first_word == "RAISERROR":
            purpose_data["purpose"] = "ERROR_HANDLING"
            purpose_data["subPurpose"] = "ERROR_RAISING"
        
        elif first_word == "PRINT":
            purpose_data["purpose"] = "AUXILIARY"
            purpose_data["subPurpose"] = "MESSAGE_OUTPUT"
        
        else:
            # For any other statement type
            purpose_data["purpose"] = "AUXILIARY"
            purpose_data["subPurpose"] = "OTHER"
        
        # Enhanced classification with sub-purposes
        self._classify_sub_purposes(statement, purpose_data)
        
        return purpose_data
    
    def _classify_sub_purposes(self, statement, purpose_data):
        """Refine the sub-purpose classification based on statement context."""
        stmt_text = statement["statementText"].upper()
        
        # Detect data filtering patterns
        if purpose_data["purpose"] == "RETRIEVAL" and "WHERE" in stmt_text:
            # Date range filtering
            if re.search(r'WHERE\b.+?\b(BETWEEN|>=?|<=?)\b.+?\b(DATE|GETDATE|DATEADD|DATEDIFF)', stmt_text, re.IGNORECASE):
                purpose_data["subPurpose"] = "DATE_RANGE_FILTER"
            # Status check filtering
            elif re.search(r'WHERE\b.+?\bSTATUS\b.+?=', stmt_text, re.IGNORECASE):
                purpose_data["subPurpose"] = "STATUS_CHECK"
            # ID-based lookup
            elif re.search(r'WHERE\b.+?ID\b.+?=', stmt_text, re.IGNORECASE):
                purpose_data["subPurpose"] = "ID_LOOKUP"
        
        # Detect variable usage patterns
        if purpose_data["purpose"] == "VARIABLE" and purpose_data["subPurpose"] == "VARIABLE_ASSIGNMENT":
            # Detect calculation assignments
            if re.search(r'=\s*[^=]*[+\-*/][^=]*', stmt_text):
                purpose_data["subPurpose"] = "CALCULATION"
            # Detect string concatenation
            elif "+" in stmt_text and ("'" in stmt_text or "N'" in stmt_text):
                purpose_data["subPurpose"] = "STRING_BUILDING"
        
        # Detect auditing/logging patterns
        if purpose_data["purpose"] == "PERSISTENCE" and purpose_data["subPurpose"] in ["SINGLE_INSERT", "BULK_INSERT"]:
            if "LOG" in stmt_text or "AUDIT" in stmt_text or "HISTORY" in stmt_text:
                purpose_data["subPurpose"] = "LOGGING"
        
        # Detect validation patterns
        if purpose_data["purpose"] == "CONTROL_FLOW" and "IF" in stmt_text:
            if "RETURN" in stmt_text or "THROW" in stmt_text or "RAISERROR" in stmt_text:
                purpose_data["subPurpose"] = "VALIDATION"
        
        return purpose_data
    
    def _extract_affected_entities(self, statement):
        """Extract entities affected by a statement."""
        affected_entities = []
        stmt_text = statement["statementText"]
        
        # Match table references from the table_references list
        for table_ref in self.table_references:
            table_name = table_ref["table"]
            if re.search(r'\b' + re.escape(table_name) + r'\b', stmt_text, re.IGNORECASE):
                affected_entities.append(table_name)
        
        # Look for variable references
        var_pattern = r'@(\w+)'
        for match in re.finditer(var_pattern, stmt_text):
            var_name = '@' + match.group(1)
            if var_name not in affected_entities:
                affected_entities.append(var_name)
        
        return affected_entities
    
    def _extract_conditions(self, statement):
        """Extract conditions from a statement."""
        conditions = []
        stmt_text = statement["statementText"]
        
        # Extract WHERE conditions
        where_pattern = r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|HAVING|;|$)'
        for match in re.finditer(where_pattern, stmt_text, re.IGNORECASE | re.DOTALL):
            where_condition = match.group(1).strip()
            # Split complex conditions
            where_parts = re.split(r'\bAND\b|\bOR\b', where_condition, flags=re.IGNORECASE)
            for part in where_parts:
                part = part.strip()
                if part:
                    conditions.append(part)
        
        # Extract IF conditions
        if_pattern = r'IF\s+(.+?)(?:BEGIN|THEN|\n)'
        for match in re.finditer(if_pattern, stmt_text, re.IGNORECASE | re.DOTALL):
            if_condition = match.group(1).strip()
            # Split complex conditions
            if_parts = re.split(r'\bAND\b|\bOR\b', if_condition, flags=re.IGNORECASE)
            for part in if_parts:
                part = part.strip()
                if part:
                    conditions.append(part)
        
        # Extract JOIN conditions
        join_pattern = r'(?:INNER|LEFT|RIGHT|FULL)\s+JOIN\s+\w+(?:\s+AS\s+\w+)?\s+ON\s+(.+?)(?:(?:INNER|LEFT|RIGHT|FULL)\s+JOIN|WHERE|GROUP BY|ORDER BY|$)'
        for match in re.finditer(join_pattern, stmt_text, re.IGNORECASE | re.DOTALL):
            join_condition = match.group(1).strip()
            # Split complex conditions
            join_parts = re.split(r'\bAND\b|\bOR\b', join_condition, flags=re.IGNORECASE)
            for part in join_parts:
                part = part.strip()
                if part:
                    conditions.append(part)
        
        return conditions

    def get_statement_groups(self):
        """
        Group statements by their functional purpose.
        
        Returns:
            Dictionary of statement groups by purpose.
        """
        groups = {}
        
        # Group statements by purpose
        for purpose_data in self.statement_purposes:
            purpose = purpose_data["purpose"]
            if purpose not in groups:
                groups[purpose] = []
            groups[purpose].append(purpose_data)
        
        return groups
    
    def get_statement_sequences(self):
        """
        Identify sequential statement patterns.
        
        Returns:
            List of statement sequences with related purposes.
        """
        sequences = []
        
        # Group statements by block
        block_statements = {}
        for purpose_data in self.statement_purposes:
            block_id = purpose_data["blockId"]
            if block_id not in block_statements:
                block_statements[block_id] = []
            block_statements[block_id].append(purpose_data)
        
        # Sort statements within each block by line number
        for block_id, statements in block_statements.items():
            statements.sort(key=lambda s: s["lineRange"][0])
        
        # Identify sequences
        for block_id, statements in block_statements.items():
            if len(statements) < 2:
                continue
            
            current_sequence = []
            for i, statement in enumerate(statements):
                if not current_sequence:
                    current_sequence.append(statement)
                    continue
                
                # Check if this statement is related to the previous one
                prev_statement = current_sequence[-1]
                
                # Check for related purposes
                related = False
                
                # Variable assignment followed by usage
                if (prev_statement["purpose"] == "VARIABLE" and 
                    statement["purpose"] in ["RETRIEVAL", "PERSISTENCE", "CONTROL_FLOW"]):
                    for entity in prev_statement["affectedEntities"]:
                        if entity in statement["affectedEntities"]:
                            related = True
                            break
                
                # Retrieval followed by persistence
                elif (prev_statement["purpose"] == "RETRIEVAL" and 
                     statement["purpose"] == "PERSISTENCE"):
                    related = True
                
                # Control flow followed by any operation
                elif prev_statement["purpose"] == "CONTROL_FLOW":
                    related = True
                
                # Check for common entities
                else:
                    common_entities = set(prev_statement["affectedEntities"]) & set(statement["affectedEntities"])
                    if common_entities:
                        related = True
                
                if related:
                    current_sequence.append(statement)
                else:
                    # End the current sequence and start a new one
                    if len(current_sequence) > 1:
                        sequence = {
                            "sequenceId": f"seq_{len(sequences)}",
                            "blockId": block_id,
                            "statements": [s["statementId"] for s in current_sequence],
                            "lineRange": [current_sequence[0]["lineRange"][0], 
                                         current_sequence[-1]["lineRange"][1]],
                            "purposes": list(set(s["purpose"] for s in current_sequence)),
                            "affectedEntities": list(set(entity for s in current_sequence 
                                                       for entity in s["affectedEntities"]))
                        }
                        sequences.append(sequence)
                    
                    current_sequence = [statement]
            
            # Handle the last sequence
            if len(current_sequence) > 1:
                sequence = {
                    "sequenceId": f"seq_{len(sequences)}",
                    "blockId": block_id,
                    "statements": [s["statementId"] for s in current_sequence],
                    "lineRange": [current_sequence[0]["lineRange"][0], 
                                 current_sequence[-1]["lineRange"][1]],
                    "purposes": list(set(s["purpose"] for s in current_sequence)),
                    "affectedEntities": list(set(entity for s in current_sequence 
                                               for entity in s["affectedEntities"]))
                }
                sequences.append(sequence)
        
        return sequences