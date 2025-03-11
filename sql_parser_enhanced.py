#!/usr/bin/env python3
"""
Enhanced SQL Stored Procedure Business Rule Parser

An enhanced Python script that parses T-SQL stored procedures to extract structural information,
business rules, and repository pattern migration information.

Usage:
    python sql_parser_enhanced.py --input procedure.sql --output analysis.json
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, List, Optional, Union, Any

from sql_parser_enhanced.sql_structure import StructureAnalyzer
from sql_parser_enhanced.sql_operations import OperationDetector
from sql_parser_enhanced.business_rules import RuleExtractor
from sql_parser_enhanced.data_flow_analyzer import DataFlowAnalyzer
from sql_parser_enhanced.statement_classifier import StatementClassifier
from sql_parser_enhanced.parameter_tracker import ParameterTracker
from sql_parser_enhanced.repository_analyzer import RepositoryAnalyzer
from sql_parser_enhanced.output_generator import OutputGenerator, clean_output_for_json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Enhanced SQL Stored Procedure Business Rule Parser"
    )
    parser.add_argument(
        "--input", "-i", required=True,
        help="Input SQL file or directory containing SQL files"
    )
    parser.add_argument(
        "--output", "-o", required=True,
        help="Output JSON file or directory for results"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--pretty", "-p", action="store_true",
        help="Pretty print JSON output"
    )
    parser.add_argument(
        "--basic", "-b", action="store_true",
        help="Run only basic analysis (faster)"
    )
    return parser.parse_args()


def read_input_file(file_path: str) -> str:
    """Read SQL content from input file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        raise


def process_single_file(input_path: str, output_path: str, pretty: bool = False, basic: bool = False) -> None:
    """Process a single SQL file and generate output"""
    try:
        # Read input SQL
        sql_content = read_input_file(input_path)
        procedure_name = os.path.splitext(os.path.basename(input_path))[0]
        
        # Initialize basic components
        structure_analyzer = StructureAnalyzer(sql_content, procedure_name)
        
        # Step 1: Analyze structure
        logger.info(f"Analyzing structure of {procedure_name}...")
        metadata = structure_analyzer.extract_metadata()
        logical_blocks = structure_analyzer.identify_blocks()
        
        # Step 2: Detect operations
        logger.info(f"Detecting data operations...")
        operation_detector = OperationDetector(sql_content, logical_blocks)
        table_references = operation_detector.detect_table_references()
        dynamic_sql_ops = operation_detector.analyze_dynamic_sql()
        temp_structures = operation_detector.analyze_temp_tables()
        
        # Step 3: Extract business rules
        logger.info(f"Extracting potential business rules...")
        rule_extractor = RuleExtractor(
            sql_content, 
            logical_blocks, 
            table_references, 
            dynamic_sql_ops,
            temp_structures
        )
        potential_rules = rule_extractor.extract_rules()
        
        if basic:
            # Generate basic output
            logger.info(f"Generating basic output...")
            output_generator = OutputGenerator(
                metadata, 
                logical_blocks, 
                table_references, 
                potential_rules
            )
            output_json = output_generator.generate_json(pretty=pretty)
        else:
            # Continue with enhanced analysis
            
            # Step 4: Analyze data flows
            logger.info(f"Analyzing data flows...")
            data_flow_analyzer = DataFlowAnalyzer(
                sql_content, 
                logical_blocks, 
                table_references, 
                dynamic_sql_ops,
                temp_structures
            )
            data_flows = data_flow_analyzer.analyze_data_flows()
            
            # Step 5: Classify statements
            logger.info(f"Classifying statements...")
            statement_classifier = StatementClassifier(
                sql_content, 
                logical_blocks, 
                table_references
            )
            statement_purposes = statement_classifier.classify_statements()
            
            # Step 6: Analyze parameter usage
            logger.info(f"Analyzing parameter usage...")
            parameter_tracker = ParameterTracker(
                sql_content, 
                logical_blocks, 
                metadata,
                statement_purposes
            )
            parameter_usage = parameter_tracker.analyze_parameter_usage()
            test_value_candidates = parameter_tracker.extract_test_values()
            
            # Step 7: Analyze repository pattern
            logger.info(f"Analyzing repository pattern...")
            repository_analyzer = RepositoryAnalyzer(
                sql_content, 
                logical_blocks, 
                table_references, 
                statement_purposes,
                data_flows,
                parameter_usage
            )
            query_patterns = repository_analyzer.detect_query_patterns()
            repository_boundaries = repository_analyzer.suggest_repository_boundaries()
            implementation_complexity = repository_analyzer.analyze_implementation_complexity()
            
            # Generate enhanced output
            logger.info(f"Generating enhanced output...")
            output_generator = OutputGenerator(
                metadata, 
                logical_blocks, 
                table_references, 
                potential_rules,
                data_flows,
                statement_purposes,
                parameter_usage,
                query_patterns,
                repository_boundaries,
                implementation_complexity,
                test_value_candidates
            )
            output_json = output_generator.generate_json(pretty=pretty)
        
        # Clean output to ensure it's JSON serializable
        output_dict = json.loads(output_json)
        cleaned_output = clean_output_for_json(output_dict)
        output_json = json.dumps(cleaned_output, indent=2 if pretty else None)
        
        # Write output
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(output_json)
        
        logger.info(f"Analysis complete. Output written to {output_path}")
    
    except Exception as e:
        logger.error(f"Error processing {input_path}: {e}")
        raise


def process_directory(input_dir: str, output_dir: str, pretty: bool = False, basic: bool = False) -> None:
    """Process all SQL files in a directory"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    for filename in os.listdir(input_dir):
        if filename.endswith('.sql'):
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}.json")
            process_single_file(input_path, output_path, pretty, basic)


def main() -> None:
    """Main entry point"""
    try:
        args = parse_arguments()
        
        # Configure verbose logging if requested
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Process input (file or directory)
        if os.path.isdir(args.input):
            if not os.path.isdir(args.output):
                logger.error("When input is a directory, output must also be a directory")
                sys.exit(1)
            process_directory(args.input, args.output, args.pretty, args.basic)
        else:
            process_single_file(args.input, args.output, args.pretty, args.basic)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()