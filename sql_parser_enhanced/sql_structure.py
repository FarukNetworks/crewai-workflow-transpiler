"""
Enhanced SQL Structure Analyzer

Identifies logical blocks and procedural structure in SQL stored procedures with enhanced relationship tracking.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any, Set
import sqlparse
from sqlparse.sql import Token, TokenList

logger = logging.getLogger(__name__)


class StructureAnalyzer:
    """
    Analyzes the structure of a SQL stored procedure to identify
    logical blocks, parameters, and other structural elements with enhanced relationship tracking.
    """

    def __init__(self, sql_content: str, procedure_name: Optional[str] = None):
        """
        Initialize with SQL content and optional procedure name
        
        Args:
            sql_content: The SQL stored procedure code
            procedure_name: Optional procedure name (will be extracted if not provided)
        """
        self.sql_content = sql_content
        self.provided_procedure_name = procedure_name
        self.parsed_statements = sqlparse.parse(sql_content)
        self.lines = sql_content.splitlines()
        self.block_stack = []
        self.current_block_id = None
        self.block_counter = 0
        self.blocks = []
        
    def extract_metadata(self) -> Dict[str, Any]:
        """
        Extract procedure metadata including name and parameters
        
        Returns:
            Dictionary containing procedure metadata
        """
        metadata = {
            "procedureName": self.provided_procedure_name or "",
            "parameters": []
        }
        
        # Look for CREATE PROCEDURE statement
        create_proc_pattern = r"CREATE\s+PROC(?:EDURE)?\s+(\[?[\w\.\[\]]+\]?)(?:\s+|$)"
        match = re.search(create_proc_pattern, self.sql_content, re.IGNORECASE)
        
        if match:
            # Extract procedure name
            proc_name = match.group(1).strip('[]')
            metadata["procedureName"] = proc_name
            
            # Extract parameters
            param_section = self.sql_content[match.end():].strip()
            param_end = param_section.find("AS")
            if param_end > 0:
                param_section = param_section[:param_end].strip()
            
            # Parse parameters
            param_pattern = r"@(\w+)\s+([\w\(\)\d\,\.]+)(?:\s*=\s*([^,\n]*))?[,\s]*"
            params = re.findall(param_pattern, param_section, re.IGNORECASE)
            
            for param_name, param_type, default_value in params:
                param = {
                    "name": f"@{param_name}",
                    "dataType": param_type.strip()
                }
                if default_value:
                    param["defaultValue"] = default_value.strip()
                metadata["parameters"].append(param)
        
        # Extract header comments
        header_comment_pattern = r"/\*\s*([\s\S]*?)\s*\*/|--\s*(.*)"
        comments = []
        for match in re.finditer(header_comment_pattern, self.sql_content[:1000]):
            if match.group(1):  # Block comment
                comments.append(match.group(1).strip())
            elif match.group(2):  # Line comment
                comments.append(match.group(2).strip())
        
        if comments:
            metadata["headerComments"] = comments
        
        return metadata
    
    def identify_blocks(self) -> List[Dict[str, Any]]:
        """
        Identify logical blocks within the SQL procedure
        
        Returns:
            List of logical blocks with their properties
        """
        # Reset state
        self.blocks = []
        self.block_stack = []
        self.block_counter = 0
        
        # Initialize with procedure body as root block
        root_block = self._create_block(
            block_type="PROCEDURE_BODY",
            parent_id=None,
            start_line=0,
            end_line=len(self.lines) - 1,
            code_text=self.sql_content
        )
        self.blocks.append(root_block)
        
        # Process parsed statements
        for statement in self.parsed_statements:
            self._process_statement(statement)
        
        # Process nested blocks
        self._identify_nested_blocks()
        
        # Process transactions
        self._identify_transactions()
        
        # Process error handling
        self._identify_error_handling()
        
        # Enhanced: identify block relationships
        self._identify_block_relationships()
        
        # Enhanced: classify block purposes
        self._classify_block_purpose()
        
        return self.blocks
    
    def _create_block(self, block_type: str, parent_id: Optional[str], 
                    start_line: int, end_line: int, code_text: str) -> Dict[str, Any]:
        """Create a new logical block entry"""
        block_id = f"block_{self.block_counter}"
        self.block_counter += 1
        
        # Extract comments associated with this block
        comments = self._extract_block_comments(code_text)
        
        return {
            "id": block_id,
            "type": block_type,
            "lineRange": [start_line, end_line],
            "parentBlock": parent_id,
            "codeText": code_text,
            "comments": comments
        }
    
    def _process_statement(self, statement: TokenList) -> None:
        """Process a SQL statement to identify blocks"""
        # Process tokens to find blocks
        self._process_tokens(statement.tokens, None)
    
    def _process_tokens(self, tokens: List[Token], parent_id: Optional[str], 
                      current_line: int = 0) -> int:
        """
        Recursively process tokens to identify logical blocks
        
        Args:
            tokens: List of SQL tokens
            parent_id: ID of parent block
            current_line: Current line number
            
        Returns:
            Updated line number after processing
        """
        i = 0
        while i < len(tokens):
            token = tokens[i]
            
            # Track line numbers
            current_line += token.value.count('\n')
            
            # Check for block start keywords
            if token.is_keyword:
                keyword = token.value.upper()
                
                # IF block
                if keyword == "IF":
                    # Find the condition and the IF block content
                    condition_tokens = []
                    j = i + 1
                    while j < len(tokens) and tokens[j].value.upper() != "BEGIN":
                        condition_tokens.append(tokens[j])
                        current_line += tokens[j].value.count('\n')
                        j += 1
                    
                    # Create IF block
                    condition_text = "".join(t.value for t in condition_tokens)
                    if_block = self._create_block(
                        block_type="IF",
                        parent_id=parent_id,
                        start_line=current_line - condition_text.count('\n'),
                        end_line=current_line,  # Will be updated when finding END
                        code_text=f"IF {condition_text}"
                    )
                    self.blocks.append(if_block)
                    
                    # Process IF block content
                    if j < len(tokens) and tokens[j].value.upper() == "BEGIN":
                        # Skip BEGIN token
                        current_line += tokens[j].value.count('\n')
                        j += 1
                        
                        # Find matching END
                        begin_count = 1
                        begin_tokens = [tokens[j-1]]  # Include BEGIN token
                        while j < len(tokens) and begin_count > 0:
                            if tokens[j].value.upper() == "BEGIN":
                                begin_count += 1
                            elif tokens[j].value.upper() == "END":
                                begin_count -= 1
                            
                            begin_tokens.append(tokens[j])
                            current_line += tokens[j].value.count('\n')
                            j += 1
                        
                        # Update IF block
                        if_block["codeText"] = f"IF {condition_text} " + "".join(t.value for t in begin_tokens)
                        if_block["lineRange"][1] = current_line
                        
                        # Skip processed tokens
                        i = j
                    else:
                        # Single statement IF
                        while j < len(tokens) and not tokens[j].value.upper() in ["ELSE", "END"]:
                            if_block["codeText"] += tokens[j].value
                            current_line += tokens[j].value.count('\n')
                            j += 1
                        
                        if_block["lineRange"][1] = current_line
                        i = j
                
                # ELSE block
                elif keyword == "ELSE":
                    # Create ELSE block
                    else_block = self._create_block(
                        block_type="ELSE",
                        parent_id=parent_id,
                        start_line=current_line,
                        end_line=current_line,  # Will be updated
                        code_text="ELSE"
                    )
                    self.blocks.append(else_block)
                    
                    # Process ELSE content
                    j = i + 1
                    if j < len(tokens) and tokens[j].value.upper() == "BEGIN":
                        # Skip BEGIN token
                        current_line += tokens[j].value.count('\n')
                        j += 1
                        
                        # Find matching END
                        begin_count = 1
                        else_tokens = ["BEGIN"]  # Include BEGIN token
                        while j < len(tokens) and begin_count > 0:
                            if tokens[j].value.upper() == "BEGIN":
                                begin_count += 1
                            elif tokens[j].value.upper() == "END":
                                begin_count -= 1
                            
                            else_tokens.append(tokens[j].value)
                            current_line += tokens[j].value.count('\n')
                            j += 1
                        
                        # Update ELSE block
                        else_block["codeText"] = "ELSE " + "".join(else_tokens)
                        else_block["lineRange"][1] = current_line
                        
                        # Skip processed tokens
                        i = j
                    else:
                        # Single statement ELSE
                        while j < len(tokens) and not tokens[j].value.upper() in ["END"]:
                            else_block["codeText"] += tokens[j].value
                            current_line += tokens[j].value.count('\n')
                            j += 1
                        
                        else_block["lineRange"][1] = current_line
                        i = j
                
                # WHILE block
                elif keyword == "WHILE":
                    # Find the condition and the WHILE block content
                    condition_tokens = []
                    j = i + 1
                    while j < len(tokens) and tokens[j].value.upper() != "BEGIN":
                        condition_tokens.append(tokens[j])
                        current_line += tokens[j].value.count('\n')
                        j += 1
                    
                    # Create WHILE block
                    condition_text = "".join(t.value for t in condition_tokens)
                    while_block = self._create_block(
                        block_type="WHILE",
                        parent_id=parent_id,
                        start_line=current_line - condition_text.count('\n'),
                        end_line=current_line,  # Will be updated
                        code_text=f"WHILE {condition_text}"
                    )
                    self.blocks.append(while_block)
                    
                    # Process WHILE block content
                    if j < len(tokens) and tokens[j].value.upper() == "BEGIN":
                        # Skip BEGIN token
                        current_line += tokens[j].value.count('\n')
                        j += 1
                        
                        # Find matching END
                        begin_count = 1
                        while_tokens = ["BEGIN"]  # Include BEGIN token
                        while j < len(tokens) and begin_count > 0:
                            if tokens[j].value.upper() == "BEGIN":
                                begin_count += 1
                            elif tokens[j].value.upper() == "END":
                                begin_count -= 1
                            
                            while_tokens.append(tokens[j].value)
                            current_line += tokens[j].value.count('\n')
                            j += 1
                        
                        # Update WHILE block
                        while_block["codeText"] = f"WHILE {condition_text} " + "".join(while_tokens)
                        while_block["lineRange"][1] = current_line
                        
                        # Skip processed tokens
                        i = j
                    else:
                        # Single statement WHILE
                        while j < len(tokens) and not tokens[j].value.upper() in ["END"]:
                            while_block["codeText"] += tokens[j].value
                            current_line += tokens[j].value.count('\n')
                            j += 1
                        
                        while_block["lineRange"][1] = current_line
                        i = j
                
                # CASE block
                elif keyword == "CASE":
                    # Find the entire CASE statement
                    case_tokens = [token]
                    j = i + 1
                    end_case_found = False
                    while j < len(tokens) and not end_case_found:
                        case_tokens.append(tokens[j])
                        if tokens[j].value.upper() == "END" and j+1 < len(tokens) and tokens[j+1].value.upper() == "CASE":
                            end_case_found = True
                            case_tokens.append(tokens[j+1])  # Include END CASE
                            j += 1
                        current_line += tokens[j].value.count('\n')
                        j += 1
                    
                    # Create CASE block
                    case_text = "".join(t.value for t in case_tokens)
                    case_block = self._create_block(
                        block_type="CASE",
                        parent_id=parent_id,
                        start_line=current_line - case_text.count('\n'),
                        end_line=current_line,
                        code_text=case_text
                    )
                    self.blocks.append(case_block)
                    
                    # Skip processed tokens
                    i = j
                
                # BEGIN-END block (without IF/WHILE/etc.)
                elif keyword == "BEGIN" and (i == 0 or tokens[i-1].value.upper() not in ["IF", "ELSE", "WHILE"]):
                    # Create BEGIN-END block
                    begin_end_block = self._create_block(
                        block_type="BEGIN_END",
                        parent_id=parent_id,
                        start_line=current_line,
                        end_line=current_line,  # Will be updated
                        code_text="BEGIN"
                    )
                    self.blocks.append(begin_end_block)
                    
                    # Find matching END
                    begin_count = 1
                    begin_tokens = ["BEGIN"]
                    j = i + 1
                    while j < len(tokens) and begin_count > 0:
                        if tokens[j].value.upper() == "BEGIN":
                            begin_count += 1
                        elif tokens[j].value.upper() == "END":
                            begin_count -= 1
                        
                        begin_tokens.append(tokens[j].value)
                        current_line += tokens[j].value.count('\n')
                        j += 1
                    
                    # Update BEGIN-END block
                    begin_end_block["codeText"] = "".join(begin_tokens)
                    begin_end_block["lineRange"][1] = current_line
                    
                    # Skip processed tokens
                    i = j
                else:
                    # Other keywords - just move to next token
                    i += 1
            else:
                # Non-keyword tokens - just move to next token
                i += 1
        
        return current_line
    
    def _identify_nested_blocks(self) -> None:
        """Identify nested relationships between blocks based on line ranges"""
        # Sort blocks by start line and then by descending end line
        # This ensures that outer blocks come before inner blocks with the same start line
        sorted_blocks = sorted(
            self.blocks, 
            key=lambda b: (b["lineRange"][0], -b["lineRange"][1])
        )
        
        # Build parent-child relationships based on line containment
        for i, block in enumerate(sorted_blocks):
            if block["parentBlock"] is not None:
                continue  # Skip blocks that already have a parent
            
            for potential_parent in sorted_blocks[:i]:
                if (potential_parent["lineRange"][0] <= block["lineRange"][0] and
                    potential_parent["lineRange"][1] >= block["lineRange"][1]):
                    block["parentBlock"] = potential_parent["id"]
                    break
    
    def _identify_transactions(self) -> None:
        """Identify transaction blocks in the SQL code"""
        # Look for BEGIN TRANSACTION, COMMIT, ROLLBACK patterns
        begin_trans_pattern = r"BEGIN\s+(?:TRAN|TRANSACTION)"
        commit_pattern = r"COMMIT\s+(?:TRAN|TRANSACTION)?"
        rollback_pattern = r"ROLLBACK\s+(?:TRAN|TRANSACTION)?"
        
        # Find BEGIN TRANSACTION statements
        for match in re.finditer(begin_trans_pattern, self.sql_content, re.IGNORECASE):
            start_pos = match.start()
            start_line = self.sql_content[:start_pos].count('\n')
            
            # Find corresponding COMMIT or ROLLBACK
            # This is simplified - in real code, you'd need more sophisticated
            # logic to handle nested transactions and conditional commits/rollbacks
            end_pos = None
            end_line = None
            for commit_match in re.finditer(commit_pattern, self.sql_content[start_pos:], re.IGNORECASE):
                end_pos = start_pos + commit_match.start()
                end_line = start_line + self.sql_content[start_pos:end_pos].count('\n')
                break
            
            if end_pos is None:
                for rollback_match in re.finditer(rollback_pattern, self.sql_content[start_pos:], re.IGNORECASE):
                    end_pos = start_pos + rollback_match.start()
                    end_line = start_line + self.sql_content[start_pos:end_pos].count('\n')
                    break
            
            if end_pos is not None:
                # Create transaction block
                transaction_code = self.sql_content[start_pos:end_pos + len("COMMIT TRANSACTION")]
                transaction_block = self._create_block(
                    block_type="TRANSACTION",
                    parent_id=None,  # Will be set in _identify_nested_blocks
                    start_line=start_line,
                    end_line=end_line,
                    code_text=transaction_code
                )
                self.blocks.append(transaction_block)
    
    def _identify_error_handling(self) -> None:
        """Identify error handling (TRY/CATCH) blocks in the SQL code"""
        # Look for BEGIN TRY, END TRY, BEGIN CATCH, END CATCH patterns
        begin_try_pattern = r"BEGIN\s+TRY"
        end_try_pattern = r"END\s+TRY"
        begin_catch_pattern = r"BEGIN\s+CATCH"
        end_catch_pattern = r"END\s+CATCH"
        
        # Find BEGIN TRY statements
        for match in re.finditer(begin_try_pattern, self.sql_content, re.IGNORECASE):
            try_start_pos = match.start()
            try_start_line = self.sql_content[:try_start_pos].count('\n')
            
            # Find corresponding END TRY
            end_try_match = re.search(end_try_pattern, self.sql_content[try_start_pos:], re.IGNORECASE)
            if end_try_match:
                try_end_pos = try_start_pos + end_try_match.end()
                try_end_line = try_start_line + self.sql_content[try_start_pos:try_end_pos].count('\n')
                
                # Create TRY block
                try_code = self.sql_content[try_start_pos:try_end_pos]
                try_block = self._create_block(
                    block_type="TRY",
                    parent_id=None,  # Will be set in _identify_nested_blocks
                    start_line=try_start_line,
                    end_line=try_end_line,
                    code_text=try_code
                )
                self.blocks.append(try_block)
                
                # Look for corresponding CATCH block
                catch_start_match = re.search(begin_catch_pattern, self.sql_content[try_end_pos:], re.IGNORECASE)
                if catch_start_match and catch_start_match.start() + try_end_pos - try_start_pos < 10:  # Ensure it's close to the END TRY
                    catch_start_pos = try_end_pos + catch_start_match.start()
                    catch_start_line = try_end_line + self.sql_content[try_end_pos:catch_start_pos].count('\n')
                    
                    # Find corresponding END CATCH
                    end_catch_match = re.search(end_catch_pattern, self.sql_content[catch_start_pos:], re.IGNORECASE)
                    if end_catch_match:
                        catch_end_pos = catch_start_pos + end_catch_match.end()
                        catch_end_line = catch_start_line + self.sql_content[catch_start_pos:catch_end_pos].count('\n')
                        
                        # Create CATCH block
                        catch_code = self.sql_content[catch_start_pos:catch_end_pos]
                        catch_block = self._create_block(
                            block_type="CATCH",
                            parent_id=None,  # Will be set in _identify_nested_blocks
                            start_line=catch_start_line,
                            end_line=catch_end_line,
                            code_text=catch_code
                        )
                        self.blocks.append(catch_block)
    
    def _extract_block_comments(self, code_text: str) -> List[str]:
        """Extract comments from a block of code"""
        comments = []
        
        # Extract block comments
        block_comment_pattern = r"/\*\s*([\s\S]*?)\s*\*/"
        for match in re.finditer(block_comment_pattern, code_text):
            comments.append(match.group(1).strip())
        
        # Extract line comments
        line_comment_pattern = r"--\s*(.*)"
        for match in re.finditer(line_comment_pattern, code_text):
            comments.append(match.group(1).strip())
        
        return comments
    
    def _identify_block_relationships(self):
        """
        Identify relationships between logical blocks including parent-child, 
        sequential, and conditional nesting relationships.
        
        This enhanced method adds childBlocks, nextBlock, and conditionBoundary fields.
        """
        # First, sort blocks by their line ranges for efficient processing
        sorted_blocks = sorted(self.blocks, key=lambda b: (b["lineRange"][0], -b["lineRange"][1]))
        
        # Create a map of block IDs to their indices for easy lookup
        block_id_map = {block["id"]: i for i, block in enumerate(self.blocks)}
        
        # Initialize relationship fields in each block
        for block in self.blocks:
            block["childBlocks"] = []
            block["nextBlock"] = None
            if block["type"] in ["IF", "WHILE", "CASE"]:
                block["conditionBoundary"] = {"startLine": block["lineRange"][0], "endLine": block["lineRange"][0], "condition": None}
        
        # Build parent-child relationships
        for i, block in enumerate(sorted_blocks):
            parent_id = block["parentBlock"]
            if parent_id and parent_id in block_id_map:
                parent_idx = block_id_map[parent_id]
                if block["id"] not in self.blocks[parent_idx]["childBlocks"]:
                    self.blocks[parent_idx]["childBlocks"].append(block["id"])
        
        # Build sequential relationships (nextBlock)
        for i, block in enumerate(sorted_blocks):
            # Find blocks at the same nesting level that follow this block
            same_level_blocks = [b for b in sorted_blocks if b["parentBlock"] == block["parentBlock"] 
                              and b["lineRange"][0] > block["lineRange"][1]]
            if same_level_blocks:
                # The next block is the one that starts immediately after this one
                next_block = min(same_level_blocks, key=lambda b: b["lineRange"][0])
                block["nextBlock"] = next_block["id"]
        
        # Extract condition boundaries and text for conditional blocks
        for block in self.blocks:
            if block["type"] == "IF":
                # Extract condition from IF statement
                if_pattern = r"IF\s+(.+?)(?:BEGIN|$)"
                match = re.search(if_pattern, block["codeText"], re.IGNORECASE | re.DOTALL)
                if match:
                    condition = match.group(1).strip()
                    end_line = block["lineRange"][0] + block["codeText"][:match.end()].count('\n')
                    block["conditionBoundary"] = {
                        "startLine": block["lineRange"][0],
                        "endLine": end_line,
                        "condition": condition
                    }
            
            elif block["type"] == "WHILE":
                # Extract condition from WHILE statement
                while_pattern = r"WHILE\s+(.+?)(?:BEGIN|$)"
                match = re.search(while_pattern, block["codeText"], re.IGNORECASE | re.DOTALL)
                if match:
                    condition = match.group(1).strip()
                    end_line = block["lineRange"][0] + block["codeText"][:match.end()].count('\n')
                    block["conditionBoundary"] = {
                        "startLine": block["lineRange"][0],
                        "endLine": end_line,
                        "condition": condition
                    }
    
    def _classify_block_purpose(self):
        """
        Classify each block by its primary purpose based on contained operations.
        """
        # Define pattern matchers for different purposes
        retrieval_patterns = [
            r'SELECT\s+(?!INTO)',  # SELECT statements (not INSERT INTO)
        ]
        
        filtering_patterns = [
            r'WHERE\s+',
            r'HAVING\s+',
            r'IF\s+(?!EXISTS)',  # IF statements (not IF EXISTS)
        ]
        
        transformation_patterns = [
            r'JOIN\s+',
            r'INNER\s+JOIN',
            r'LEFT\s+JOIN',
            r'RIGHT\s+JOIN',
            r'CROSS\s+JOIN',
            r'UNION',
            r'GROUP\s+BY',
            r'ORDER\s+BY',
            r'PIVOT',
            r'CASE\s+WHEN',
        ]
        
        insertion_patterns = [
            r'INSERT\s+INTO',
            r'UPDATE\s+',
            r'DELETE\s+FROM',
            r'MERGE\s+INTO',
        ]
        
        control_flow_patterns = [
            r'BEGIN\s+TRANSACTION',
            r'COMMIT',
            r'ROLLBACK',
            r'RETURN',
            r'THROW',
            r'RAISERROR',
            r'TRY',
            r'CATCH',
        ]
        
        for block in self.blocks:
            # Check for each purpose type
            if block["type"] in ["TRY", "CATCH"]:
                block["purpose"] = "ERROR_HANDLING"
            elif block["type"] == "TRANSACTION":
                block["purpose"] = "TRANSACTION_CONTROL"
            elif any(re.search(pattern, block["codeText"], re.IGNORECASE) for pattern in insertion_patterns):
                block["purpose"] = "DATA_INSERTION"
            elif any(re.search(pattern, block["codeText"], re.IGNORECASE) for pattern in transformation_patterns):
                block["purpose"] = "DATA_TRANSFORMATION"
            elif any(re.search(pattern, block["codeText"], re.IGNORECASE) for pattern in filtering_patterns):
                block["purpose"] = "DATA_FILTERING"
            elif any(re.search(pattern, block["codeText"], re.IGNORECASE) for pattern in retrieval_patterns):
                block["purpose"] = "DATA_RETRIEVAL"
            elif any(re.search(pattern, block["codeText"], re.IGNORECASE) for pattern in control_flow_patterns):
                block["purpose"] = "CONTROL_FLOW"
            else:
                block["purpose"] = "AUXILIARY"