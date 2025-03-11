"""
Enhanced Output Generator

Generates structured JSON output from parsing results with enhanced repository pattern information.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple, Any, Set

logger = logging.getLogger(__name__)


class OutputGenerator:
    """
    Generates structured JSON output from parsing results.
    """
    
    def __init__(self, metadata: Dict[str, Any], 
                logical_blocks: List[Dict[str, Any]], 
                table_references: List[Dict[str, Any]], 
                potential_rules: List[Dict[str, Any]],
                data_flows: List[Dict[str, Any]] = None,
                statement_purposes: List[Dict[str, Any]] = None,
                parameter_usage: List[Dict[str, Any]] = None,
                query_patterns: List[Dict[str, Any]] = None,
                repository_boundaries: List[Dict[str, Any]] = None,
                implementation_complexity: List[Dict[str, Any]] = None,
                test_value_candidates: List[Dict[str, Any]] = None):
        """
        Initialize with parsing results.
        
        Args:
            metadata: Procedure metadata
            logical_blocks: Identified logical blocks
            table_references: Detected table references
            potential_rules: Extracted potential business rules
            data_flows: Data flow analysis (optional)
            statement_purposes: Statement purpose classifications (optional)
            parameter_usage: Parameter usage data (optional)
            query_patterns: Query pattern analysis (optional)
            repository_boundaries: Repository method boundary suggestions (optional)
            implementation_complexity: Implementation complexity indicators (optional)
            test_value_candidates: Test value candidates (optional)
        """
        self.metadata = metadata
        self.logical_blocks = logical_blocks
        self.table_references = table_references
        self.potential_rules = potential_rules
        self.data_flows = data_flows or []
        self.statement_purposes = statement_purposes or []
        self.parameter_usage = parameter_usage or []
        self.query_patterns = query_patterns or []
        self.repository_boundaries = repository_boundaries or []
        self.implementation_complexity = implementation_complexity or []
        self.test_value_candidates = test_value_candidates or []
    
    def generate_json(self, pretty: bool = False) -> str:
        """
        Generate JSON output from parsing results.
        
        Args:
            pretty: Whether to format the JSON for readability
        
        Returns:
            JSON string representation of the parsing results
        """
        # Basic output
        output = {
            "metadata": self.metadata,
            "logicalBlocks": self.logical_blocks,
            "tableReferences": self.table_references,
            "potentialBusinessRules": self.potential_rules
        }
        
        # Enhanced output
        if self.data_flows:
            output["dataFlow"] = self.data_flows
        
        if self.statement_purposes:
            output["statementPurpose"] = self.statement_purposes
        
        if self.parameter_usage:
            output["parameterUsage"] = self.parameter_usage
        
        if self.query_patterns:
            output["queryPatterns"] = self.query_patterns
        
        if self.repository_boundaries:
            output["repositoryBoundaries"] = self.repository_boundaries
        
        if self.implementation_complexity:
            output["implementationComplexity"] = self.implementation_complexity
        
        if self.test_value_candidates:
            output["testValueCandidates"] = self.test_value_candidates
        
        # Generate JSON string
        indent = 2 if pretty else None
        return json.dumps(output, indent=indent)


def clean_output_for_json(output_dict: Dict) -> Dict:
    """
    Clean up the output dictionary to ensure it's JSON serializable.
    
    Args:
        output_dict: Dictionary to clean
    
    Returns:
        Cleaned dictionary
    """
    if isinstance(output_dict, dict):
        result = {}
        for key, value in output_dict.items():
            # Skip keys with None values
            if value is None:
                continue
            
            # Process each key-value pair
            if isinstance(value, dict):
                result[key] = clean_output_for_json(value)
            elif isinstance(value, list):
                result[key] = [clean_output_for_json(item) if isinstance(item, (dict, list)) else item for item in value]
            elif isinstance(value, (str, int, float, bool)):
                result[key] = value
            else:
                # Convert other types to string
                result[key] = str(value)
        return result
    elif isinstance(output_dict, list):
        return [clean_output_for_json(item) if isinstance(item, (dict, list)) else item for item in output_dict]
    else:
        return output_dict


def merge_outputs(original_output: Dict, enhanced_output: Dict) -> Dict:
    """
    Merge original parser output with enhanced parser output.
    
    Args:
        original_output: Original parser output
        enhanced_output: Enhanced parser output
    
    Returns:
        Merged output
    """
    # Start with the original output
    merged = original_output.copy()
    
    # Add or replace with enhanced output sections
    for key, value in enhanced_output.items():
        if key not in merged:
            merged[key] = value
        elif isinstance(value, list) and isinstance(merged[key], list):
            # Merge lists by appending new items
            existing_ids = set()
            if key in ["logicalBlocks", "tableReferences", "potentialBusinessRules"]:
                # These sections use "id" field for identification
                existing_ids = {item.get("id") for item in merged[key] if "id" in item}
                merged[key].extend([item for item in value if "id" in item and item["id"] not in existing_ids])
            else:
                # For other sections, just append everything
                merged[key].extend(value)
        elif isinstance(value, dict) and isinstance(merged[key], dict):
            # Recursively merge dictionaries
            merged[key] = merge_outputs(merged[key], value)
    
    return merged