# Enhanced SQL Parser for Repository Pattern Migration

This tool is an enhanced SQL stored procedure parser that provides detailed information to assist with repository pattern migration. The parser not only extracts business rules but also identifies data flows, parameter usage, query patterns, and suggests repository method boundaries.

## Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

### Setup

1. **Clone the repository or create a project directory**

```bash
mkdir -p sql-parser-enhanced
cd sql-parser-enhanced
```

2. **Create a virtual environment (recommended)**

```bash
python -m venv venv

# On macOS/Linux
source venv/bin/activate

# On Windows
venv\Scripts\activate
```

3. **Install dependencies**

```bash
pip install sqlparse
```

4. **Create the project structure**

```
sql-parser-enhanced/
├── sql_parser_enhanced.py           # Main executable
├── sql_structure.py                 # Structure analyzer
├── sql_operations.py                # Operations detector
├── business_rules.py                # Business rule extractor
├── data_flow_analyzer.py            # Data flow analyzer
├── statement_classifier.py          # Statement classifier
├── parameter_tracker.py             # Parameter usage tracker
├── repository_analyzer.py           # Repository pattern analyzer
└── output_generator.py              # Output generator
```

5. **Make the parser executable (Unix-like systems)**

```bash
chmod +x sql_parser_enhanced.py
```

## Usage

### Basic Command

```bash
python sql_parser_enhanced.py --input <input-file.sql> --output <output-file.json>
python3 sql_parser_enhanced.py --input sp.sql --output analysis.json --pretty
```

### Options

- `--input` or `-i`: Input SQL file or directory (required)
- `--output` or `-o`: Output JSON file or directory (required)
- `--pretty` or `-p`: Format JSON output for readability
- `--verbose` or `-v`: Enable detailed logging
- `--basic` or `-b`: Run only basic analysis (faster)

### Examples

#### Analyze a single file:

```bash
python sql_parser_enhanced.py -i procedures/GetOrderDetails.sql -o analysis/GetOrderDetails.json --pretty
```

#### Analyze all SQL files in a directory:

```bash
python sql_parser_enhanced.py -i procedures/ -o analysis/ --pretty
```

#### Run with verbose logging:

```bash
python sql_parser_enhanced.py -i procedures/GetOrderDetails.sql -o analysis/GetOrderDetails.json --pretty --verbose
```

## Output

The parser generates a JSON file with detailed information about the SQL stored procedure, including:

1. **Metadata**: Procedure name, parameters, etc.
2. **Logical Blocks**: Code blocks with their relationships and purpose
3. **Table References**: Tables and columns referenced in the procedure
4. **Business Rules**: Potential business rules extracted from the code
5. **Data Flow**: How data flows between entities in the procedure
6. **Statement Purpose**: Classification of each SQL statement by purpose
7. **Parameter Usage**: How parameters are used throughout the procedure
8. **Query Patterns**: Common query patterns detected in the code
9. **Repository Boundaries**: Suggested boundaries for repository methods
10. **Implementation Complexity**: Areas that may be challenging to migrate
11. **Test Value Candidates**: Suggested test values for parameters

## Example Output Structure

```json
{
  "metadata": {
    "procedureName": "GetOrderDetails",
    "parameters": [
      {
        "name": "@OrderID",
        "dataType": "INT"
      }
    ]
  },
  "logicalBlocks": [...],
  "tableReferences": [...],
  "potentialBusinessRules": [...],
  "dataFlow": [...],
  "statementPurpose": [...],
  "parameterUsage": [...],
  "queryPatterns": [...],
  "repositoryBoundaries": [...],
  "implementationComplexity": [...],
  "testValueCandidates": [...]
}
```

## Troubleshooting

### Common Issues

1. **Missing dependencies**:

```bash
pip install sqlparse
```

2. **Permission denied when running the script**:

```bash
chmod +x sql_parser_enhanced.py
```

3. **JSON serialization errors**:

The parser includes a cleanup function to ensure output is JSON serializable. If you still encounter errors, check for non-standard data types in your output.

4. **Memory issues with large procedures**:

Use the `--basic` flag for large procedures to run only basic analysis:

```bash
python sql_parser_enhanced.py -i large_procedure.sql -o analysis.json --basic
```

## Understanding the Enhanced Output

### Repository Boundaries

The `repositoryBoundaries` section suggests logical boundaries for repository methods based on the procedure's structure:

```json
"repositoryBoundaries": [
  {
    "methodId": "method_0",
    "suggestedName": "GetOrdersByCustomerId",
    "description": "Retrieve Orders data based on filters",
    "relatedBlocks": ["block_1", "block_2"],
    "inputParameters": [
      {
        "name": "@CustomerID",
        "type": "INT",
        "usage": "FILTER_CONDITION"
      }
    ],
    "returnDataStructure": {
      "primaryEntity": "Orders",
      "includedColumns": ["OrderID", "OrderDate", "TotalAmount"],
      "returnType": "ENTITY_LIST"
    }
  }
]
```

### Implementation Complexity

The `implementationComplexity` section identifies areas that may be challenging to migrate:

```json
"implementationComplexity": [
  {
    "complexityId": "complex_0",
    "blockIds": ["block_3"],
    "complexityLevel": "HIGH",
    "complexityType": "DYNAMIC_SQL",
    "description": "Dynamic SQL construction",
    "migrationApproach": "Use IQueryable with expression building",
    "alternativeApproaches": [
      "Use stored procedure with Dapper",
      "Implement custom SQL builder"
    ]
  }
]
```

### Data Flow

The `dataFlow` section shows how data flows between entities:

```json
"dataFlow": [
  {
    "flowId": "flow_0",
    "sourceEntities": ["Orders", "OrderDetails"],
    "intermediateEntities": ["#TempOrders"],
    "targetEntities": ["@OrderTotal"],
    "operations": ["JOIN", "FILTER", "AGGREGATE"],
    "blockIds": ["block_1", "block_2"]
  }
]
```

## Repository Pattern Migration Process

Use the parser output to guide your repository pattern migration:

1. **Start with repository boundaries**: Use the suggested method boundaries as a starting point
2. **Review data flows**: Understand how data flows between entities
3. **Examine parameter usage**: Understand how parameters are used
4. **Address complexity issues**: Plan how to handle complex areas
5. **Define repository interfaces**: Based on the suggested method boundaries
6. **Implement repository methods**: Using the query patterns as a guide
7. **Add test cases**: Using the suggested test values