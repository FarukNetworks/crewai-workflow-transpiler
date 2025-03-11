"""
Business Rules Extractor

Identifies and extracts potential business rules from SQL stored procedures.
"""

import re
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


class RuleExtractor:
    """
    Extracts potential business rules from SQL stored procedures.
    """
    
    def __init__(self, sql_content, logical_blocks, table_references, 
                dynamic_sql_ops=None, temp_structures=None):
        """
        Initialize with SQL content and parsed structural information
        """
        self.sql_content = sql_content
        self.logical_blocks = logical_blocks
        self.table_references = table_references
        self.dynamic_sql_ops = dynamic_sql_ops or []
        self.temp_structures = temp_structures or []
        self.rule_counter = 0
    
    def extract_rules(self) -> List[Dict[str, Any]]:
        """
        Extract potential business rules from the SQL procedure
        
        Returns:
            List of potential business rules
        """
        potential_rules = []
        
        # Extract validation rules (IF conditions checking data values)
        validation_rules = self._extract_validation_rules()
        potential_rules.extend(validation_rules)
        
        # Extract calculation rules
        calculation_rules = self._extract_calculation_rules()
        potential_rules.extend(calculation_rules)
        
        # Extract process flow rules
        process_rules = self._extract_process_flow_rules()
        potential_rules.extend(process_rules)
        
        # Extract data integrity rules
        integrity_rules = self._extract_data_integrity_rules()
        potential_rules.extend(integrity_rules)
        
        # Extract security rules
        security_rules = self._extract_security_rules()
        potential_rules.extend(security_rules)
        
        # Extract timing rules
        timing_rules = self._extract_timing_rules()
        potential_rules.extend(timing_rules)
        
        return potential_rules
    
    def _create_rule(self, category, description, block_ids, 
                    condition=None, action=None, entities=None, 
                    business_terms=None, code_snippet=None) -> Dict[str, Any]:
        """Create a new business rule entry"""
        rule_id = f"rule_{self.rule_counter}"
        self.rule_counter += 1
        
        return {
            "ruleId": rule_id,
            "category": category,
            "description": description,
            "blockIds": block_ids,
            "condition": condition,
            "action": action,
            "entities": entities or [],
            "businessTerms": business_terms or [],
            "codeSnippet": code_snippet
        }
    
    def _extract_validation_rules(self) -> List[Dict[str, Any]]:
        """Extract validation rules (IF conditions checking data values)"""
        rules = []
        
        # Look for IF blocks that might contain validation logic
        for block in self.logical_blocks:
            if block["type"] == "IF":
                code_text = block["codeText"]
                
                # Check if this is likely a validation check
                if re.search(r'IF\s+(?:@?\w+(?:\.\w+)?)\s*(?:IS\s+(?:NOT\s+)?NULL|[<>=!]+)', code_text, re.IGNORECASE):
                    # Extract condition
                    condition_match = re.search(r'IF\s+(.*?)(?:BEGIN|$)', code_text, re.IGNORECASE | re.DOTALL)
                    condition = condition_match.group(1).strip() if condition_match else None
                    
                    # Extract action
                    action = None
                    if "RETURN" in code_text.upper():
                        action = "RETURN (validation failure)"
                    elif "RAISERROR" in code_text.upper() or "THROW" in code_text.upper():
                        action = "Raise error (validation failure)"
                    
                    # Extract entities (tables/columns referenced)
                    entities = []
                    for table_ref in self.table_references:
                        if table_ref.get("blockId") == block["id"]:
                            entities.append(table_ref["table"])
                    
                    # Look for column references in the condition
                    column_pattern = r'@?(\w+)(?:\.\w+)?'
                    for match in re.finditer(column_pattern, condition or "", re.IGNORECASE):
                        param_name = match.group(1)
                        if not param_name.upper() in ["IF", "IS", "NOT", "NULL", "AND", "OR", "IN"]:
                            entities.append(param_name)
                    
                    # Create validation rule
                    rule = self._create_rule(
                        category="VALIDATION",
                        description=f"Validation check: {condition}",
                        block_ids=[block["id"]],
                        condition=condition,
                        action=action,
                        entities=list(set(entities)),
                        code_snippet=code_text
                    )
                    rules.append(rule)
        
        return rules
    
    def _extract_calculation_rules(self) -> List[Dict[str, Any]]:
        """Extract calculation rules (formulas, arithmetic operations)"""
        rules = []
        
        # Look for calculation patterns in all blocks
        calculation_pattern = r'(?:SET|SELECT)\s+@?(\w+)\s*=\s*([^;]+?(?:[+\-*/][^;]+?)+)(?:;|$)'
        for match in re.finditer(calculation_pattern, self.sql_content, re.IGNORECASE):
            var_name = match.group(1)
            formula = match.group(2).strip()
            
            # Skip simple assignments
            if not re.search(r'[+\-*/]', formula):
                continue
            
            # Find containing block
            block_ids = []
            for block in self.logical_blocks:
                start_pos = self.sql_content.find(block["codeText"])
                end_pos = start_pos + len(block["codeText"])
                if start_pos <= match.start() and end_pos >= match.end():
                    block_ids.append(block["id"])
            
            # Extract entities
            entities = [var_name]
            term_pattern = r'@?(\w+)'
            for term_match in re.finditer(term_pattern, formula, re.IGNORECASE):
                term = term_match.group(1)
                if not term.upper() in ["AND", "OR", "IN", "IS", "NOT", "NULL"]:
                    entities.append(term)
            
            # Create calculation rule
            rule = self._create_rule(
                category="CALCULATION",
                description=f"Calculation: {var_name} = {formula}",
                block_ids=block_ids,
                action=f"Calculate {var_name}",
                entities=list(set(entities)),
                code_snippet=match.group(0)
            )
            rules.append(rule)
        
        return rules
    
    def _extract_process_flow_rules(self) -> List[Dict[str, Any]]:
        """Extract process flow rules (conditions determining processing paths)"""
        rules = []
        
        # Look for IF/ELSE blocks with significant processing logic
        for block in self.logical_blocks:
            if block["type"] == "IF":
                code_text = block["codeText"]
                
                # Skip simple validation checks already covered
                if re.search(r"IF\s+(?:@?\w+(?:\.\w+)?)\s*(?:IS\s+(?:NOT\s+)?NULL|[<>=!]+)\s*(?:RETURN|RAISERROR|THROW)", 
                          code_text, re.IGNORECASE):
                    continue
                
                # Check if this contains significant processing logic
                if (("SELECT" in code_text.upper() and ("INTO" in code_text.upper() or "FROM" in code_text.upper())) or
                    "INSERT" in code_text.upper() or "UPDATE" in code_text.upper() or "DELETE" in code_text.upper()):
                    
                    # Extract condition
                    condition_match = re.search(r"IF\s+(.*?)(?:BEGIN|$)", code_text, re.IGNORECASE | re.DOTALL)
                    condition = condition_match.group(1).strip() if condition_match else None
                    
                    # Extract entities
                    entities = []
                    for table_ref in self.table_references:
                        if table_ref.get("blockId") == block["id"]:
                            entities.append(table_ref["table"])
                    
                    # Extract operations
                    operations = []
                    if "SELECT" in code_text.upper():
                        operations.append("SELECT")
                    if "INSERT" in code_text.upper():
                        operations.append("INSERT")
                    if "UPDATE" in code_text.upper():
                        operations.append("UPDATE")
                    if "DELETE" in code_text.upper():
                        operations.append("DELETE")
                    
                    # Create process flow rule
                    rule = self._create_rule(
                        category="PROCESS_FLOW",
                        description=f"Process flow control: {condition}",
                        block_ids=[block["id"]],
                        condition=condition,
                        action=f"Execute {', '.join(operations)} operations",
                        entities=list(set(entities)),
                        code_snippet=code_text
                    )
                    rules.append(rule)
        
        return rules
    
    def _extract_data_integrity_rules(self) -> List[Dict[str, Any]]:
        """Extract data integrity rules (checks ensuring data consistency)"""
        rules = []
        
        # Look for EXISTS/NOT EXISTS checks
        exists_pattern = r'(?:EXISTS|NOT\s+EXISTS)\s*\(\s*SELECT.*?FROM\s+(\w+)'
        for match in re.finditer(exists_pattern, self.sql_content, re.IGNORECASE):
            table_name = match.group(1)
            
            # Find containing block
            block_ids = []
            for block in self.logical_blocks:
                start_pos = self.sql_content.find(block["codeText"])
                end_pos = start_pos + len(block["codeText"])
                if start_pos <= match.start() and end_pos >= match.end():
                    block_ids.append(block["id"])
            
            # Extract context
            context = self.sql_content[max(0, match.start() - 50):min(len(self.sql_content), match.end() + 50)]
            
            # Create data integrity rule
            rule = self._create_rule(
                category="DATA_INTEGRITY",
                description=f"Data integrity check on {table_name}",
                block_ids=block_ids,
                condition=f"Checking existence in {table_name}",
                entities=[table_name],
                code_snippet=context
            )
            rules.append(rule)
        
        # Look for foreign key checks
        fk_pattern = r'(\w+)\.\w+\s*=\s*(\w+)\.\w+'
        for match in re.finditer(fk_pattern, self.sql_content, re.IGNORECASE):
            table1 = match.group(1)
            table2 = match.group(2)
            
            # Skip self-references
            if table1 == table2:
                continue
            
            # Find containing block
            block_ids = []
            for block in self.logical_blocks:
                start_pos = self.sql_content.find(block["codeText"])
                end_pos = start_pos + len(block["codeText"])
                if start_pos <= match.start() and end_pos >= match.end():
                    block_ids.append(block["id"])
            
            # Extract context
            context = self.sql_content[max(0, match.start() - 50):min(len(self.sql_content), match.end() + 50)]
            
            # Create data integrity rule
            rule = self._create_rule(
                category="DATA_INTEGRITY",
                description=f"Referential integrity between {table1} and {table2}",
                block_ids=block_ids,
                condition=match.group(0),
                entities=[table1, table2],
                code_snippet=context
            )
            rules.append(rule)
        
        return rules
    
    def _extract_security_rules(self) -> List[Dict[str, Any]]:
        """Extract security rules (access control, authorization checks)"""
        rules = []
        
        # Look for security-related checks
        security_patterns = [
            r'(?:IS_MEMBER|HAS_PERMS_BY_NAME|SUSER_SNAME|USER_NAME|IS_SRVROLEMEMBER)\s*\(',
            r'EXECUTE\s+AS\s+(?:OWNER|USER)',
            r'DENY\s+|GRANT\s+|REVOKE\s+'
        ]
        
        for pattern in security_patterns:
            for match in re.finditer(pattern, self.sql_content, re.IGNORECASE):
                # Find containing block
                block_ids = []
                for block in self.logical_blocks:
                    start_pos = self.sql_content.find(block["codeText"])
                    end_pos = start_pos + len(block["codeText"])
                    if start_pos <= match.start() and end_pos >= match.end():
                        block_ids.append(block["id"])
                
                # Extract context
                context = self.sql_content[max(0, match.start() - 50):min(len(self.sql_content), match.end() + 100)]
                
                # Create security rule
                rule = self._create_rule(
                    category="SECURITY",
                    description="Security/authorization check",
                    block_ids=block_ids,
                    code_snippet=context
                )
                rules.append(rule)
        
        return rules
    
    def _extract_timing_rules(self) -> List[Dict[str, Any]]:
        """Extract timing rules (date/time-based conditions, scheduling logic)"""
        rules = []
        
        # Look for date/time comparisons
        timing_patterns = [
            r'(?:GETDATE|CURRENT_TIMESTAMP|DATEADD|DATEDIFF)\s*\(',
            r'(?:YEAR|MONTH|DAY|HOUR|MINUTE)\s*\(',
            r'@\w+\s*(?:>|<|=|>=|<=|<>|!=)\s*\d{4}-\d{2}-\d{2}'
        ]
        
        for pattern in timing_patterns:
            for match in re.finditer(pattern, self.sql_content, re.IGNORECASE):
                # Find containing block
                block_ids = []
                for block in self.logical_blocks:
                    start_pos = self.sql_content.find(block["codeText"])
                    end_pos = start_pos + len(block["codeText"])
                    if start_pos <= match.start() and end_pos >= match.end():
                        block_ids.append(block["id"])
                
                # Extract context
                context = self.sql_content[max(0, match.start() - 50):min(len(self.sql_content), match.end() + 50)]
                
                # Create timing rule
                rule = self._create_rule(
                    category="TIMING",
                    description="Time-based condition or calculation",
                    block_ids=block_ids,
                    code_snippet=context
                )
                rules.append(rule)
        
        # Look for specific date/time logic in conditional statements
        date_condition_pattern = r'IF\s+(.+?(?:GETDATE|CURRENT_TIMESTAMP|DATEADD|DATEDIFF|DATE|DAY|MONTH|YEAR).+?)(?:BEGIN|THEN|\n)'
        for match in re.finditer(date_condition_pattern, self.sql_content, re.IGNORECASE | re.DOTALL):
            condition = match.group(1).strip()
            
            # Find containing block
            block_ids = []
            for block in self.logical_blocks:
                start_pos = self.sql_content.find(block["codeText"])
                end_pos = start_pos + len(block["codeText"])
                if start_pos <= match.start() and end_pos >= match.end():
                    block_ids.append(block["id"])
            
            # Create timing rule
            rule = self._create_rule(
                category="TIMING",
                description="Date/time-based condition",
                block_ids=block_ids,
                condition=condition,
                code_snippet=match.group(0)
            )
            rules.append(rule)
        
        return rules