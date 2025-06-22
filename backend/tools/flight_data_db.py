import duckdb
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import os
import logging
from pathlib import Path
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FlightDataDBError(Exception):
    """Base exception class for FlightDataDB errors"""
    pass

class DatabaseConnectionError(FlightDataDBError):
    """Raised when there are issues with database connections"""
    pass

class DataValidationError(FlightDataDBError):
    """Raised when data validation fails"""
    pass

class FlightDataDB:
    def __init__(self, db_dir: str = "flight_data"):
        try:
            self.db_dir = Path(db_dir)
            self.connections: Dict[str, duckdb.DuckDBPyConnection] = {}
            self.message_tables: Dict[str, set] = {}
            # Create directory if it doesn't exist
            self.db_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Initialized FlightDataDB with directory: {self.db_dir}")
        except Exception as e:
            logger.error(f"Failed to initialize FlightDataDB: {str(e)}")
            raise FlightDataDBError(f"Failed to initialize database: {str(e)}")

    def _get_connection(self, session_id: str) -> duckdb.DuckDBPyConnection:
        if not session_id or not isinstance(session_id, str):
            raise DataValidationError("Invalid session_id: must be a non-empty string")
        
        try:
            if session_id not in self.connections:
                logger.debug(f"Creating new connection for session {session_id}")
                db_path = self.db_dir / f"{session_id}.db"
                logger.debug(f"Database path: {db_path}")
                
                try:
                    self.connections[session_id] = duckdb.connect(str(db_path))
                    self.message_tables[session_id] = set()
                except duckdb.Error as e:
                    raise DatabaseConnectionError(f"Failed to create database connection: {str(e)}")
                
            return self.connections[session_id]
        except Exception as e:
            logger.error(f"Error getting connection for session {session_id}: {str(e)}")
            raise DatabaseConnectionError(f"Failed to get database connection: {str(e)}")

    def _infer_duckdb_type(self, sample: Any) -> str:
        """
        Infers the DuckDB type for a given sample value.

        Args:
            sample (Any): The sample value to infer the type of.

        Returns:
            str: The DuckDB type for the sample value.
        """
        try:
            if isinstance(sample, int):
                return "BIGINT"
            elif isinstance(sample, float):
                return "DOUBLE"
            elif isinstance(sample, str):
                return "VARCHAR"
            elif isinstance(sample, bool):
                return "BOOLEAN"
            elif isinstance(sample, list):
                # If it's a list, we need to determine what type it should be
                if len(sample) == 0:
                    return "VARCHAR"  # Empty list as JSON string
                
                # Check if it's a list of arrays (like time_unix_usec)
                if isinstance(sample[0], list) and len(sample[0]) > 0:
                    first_element = sample[0][0]
                    if isinstance(first_element, (int, float)):
                        return "BIGINT"
                    else:
                        return "VARCHAR"
                
                # Check if it's a simple list with consistent types
                if all(isinstance(item, (int, float)) for item in sample):
                    return "BIGINT"  # Assume first element type
                elif all(isinstance(item, str) for item in sample):
                    return "VARCHAR"
                else:
                    # Mixed types or complex structures, store as JSON
                    return "VARCHAR"
            else:
                logger.warning(f"Unknown type for sample value: {type(sample)}, defaulting to VARCHAR")
                return "VARCHAR"
        except Exception as e:
            logger.error(f"Error inferring DuckDB type: {str(e)}")
            raise DataValidationError(f"Failed to infer data type: {str(e)}")

    def _process_field_value(self, value: Any, field: str, msg_name: str) -> Any:
        """
        Processes a field value to ensure it's in the correct format for database storage.
        
        Args:
            value (Any): The raw value to process
            field (str): The field name for context
            msg_name (str): The message name for context
            
        Returns:
            Any: The processed value ready for database insertion
        """
        try:
            if value is None:
                return None
                
            if isinstance(value, list):
                # Handle empty lists
                if len(value) == 0:
                    return None
                
                # Special handling for time_unix_usec which comes as a list of arrays
                if field == "time_unix_usec" and len(value) > 0 and isinstance(value[0], list):
                    if not value[0] or not isinstance(value[0][0], (int, float)):
                        raise DataValidationError(
                            f"Invalid time_unix_usec format in message {msg_name}: expected list of numeric arrays"
                        )
                    return value[0][0]  # Take the first element of the first array
                
                # Handle lists that should be converted to single values
                if len(value) == 1 and isinstance(value[0], (int, float, str, bool)):
                    return value[0]
                
                # For other lists, convert to JSON string
                try:
                    return json.dumps(value)
                except (TypeError, ValueError) as e:
                    raise DataValidationError(
                        f"Failed to serialize list data for field '{field}' in message {msg_name}: {str(e)}"
                    )
            
            # For non-list values, return as-is if they're valid types
            if isinstance(value, (int, float, str, bool)):
                return value
            
            # For any other type, try to convert to string
            return str(value)
            
        except Exception as e:
            logger.error(f"Error processing field value for {field} in {msg_name}: {str(e)}")
            raise DataValidationError(f"Failed to process field value: {str(e)}")

    def _create_table_for_message(self, session_id: str, msg_name: str, fields: List[str], sample_row: Dict[str, Any]) -> None:
        """
        Creates a table for a given message in the database.

        Args:
            session_id (str): The session ID to create the table for.
            msg_name (str): The name of the message to create the table for.
            fields (List[str]): The fields to create the table for.
            sample_row (Dict[str, Any]): The sample row to use to infer the types of the fields.

        """

        if not all([session_id, msg_name, fields, sample_row]):
            raise DataValidationError("Missing required parameters for table creation")
        
        try:
            logger.debug(f"Creating table for message: {msg_name}")
            logger.debug(f"Session ID: {session_id}")
            logger.debug(f"Fields to create: {fields}")
            # logger.debug(f"Sample row: {sample_row}")
            
            conn = self._get_connection(session_id)
            columns = []
            
            for field in fields:
                if field not in sample_row:
                    raise DataValidationError(f"Field '{field}' not found in sample row")
                
                duckdb_type = self._infer_duckdb_type(sample_row[field])
                if field.lower() in ("timeus", "time_boot_ms", "timestamp"):
                    duckdb_type = "BIGINT"
                # logger.debug(f"Field '{field}' inferred as type: {duckdb_type}")
                columns.append(f'"{field}" {duckdb_type}')
            
            sql = f'CREATE TABLE IF NOT EXISTS "{msg_name}" ({", ".join(columns)})'
            
            try:
                conn.execute(sql)
                self.message_tables[session_id].add(msg_name)
                logger.debug(f"Successfully created table '{msg_name}'")
            except duckdb.Error as e:
                raise DatabaseConnectionError(f"Failed to create table: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error creating table for message {msg_name}: {str(e)}")
            raise FlightDataDBError(f"Failed to create table: {str(e)}")

    def _get_message_description(self, msg_name: str) -> Optional[str]:
        """
        Gets the description of a given message from the knowledge base.

        Args:
            msg_name (str): The name of the message to get the description of.
        """
        if not msg_name or not isinstance(msg_name, str):
            raise DataValidationError("Invalid msg_name: must be a non-empty string")
        
        try:
            # Read the knowledge base file
            knowledge_base_path = os.path.join(os.path.dirname(__file__), '..', 'knowledge_base', 'knowledge_base.txt')
            
            with open(knowledge_base_path, 'r') as f:
                content = f.read()
                
            # Find the section for this message'
            if f'### {msg_name}' not in content:
                logger.warning(f"No description found for message {msg_name}")
                return ""
            
            msg_section = content.split(f'### {msg_name} ')[1].split('###')[0]
            return msg_section
            
        except FileNotFoundError:
            logger.warning(f"Knowledge base file not found at {knowledge_base_path}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting message description for {msg_name}: {str(e)}")
            raise FlightDataDBError(f"Failed to get message description: {str(e)}")

    def store_flight_data(self, session_id: str, parsed_json: Dict[str, Dict[str, List[Any]]], start_time: Optional[datetime] = None) -> None:
        """
        Stores flight data in the database.

        Args:
            session_id (str): The session ID to store the flight data for.
            parsed_json (Dict[str, Dict[str, List[Any]]]): The parsed JSON data to store.
            start_time (Optional[datetime]): The start time of the flight data.
        """
        if not session_id or not isinstance(session_id, str):
            raise DataValidationError("Invalid session_id: must be a non-empty string")
        if not parsed_json or not isinstance(parsed_json, dict):
            raise DataValidationError("Invalid parsed_json: must be a non-empty dictionary")
        
        try:
            conn = self._get_connection(session_id)
            logger.info(f"Storing flight data for session {session_id}")
            logger.debug(f"Parsed JSON keys: {parsed_json.keys()}")

            for msg_name, msg_data in parsed_json.items():
                if not msg_data or not isinstance(msg_data, dict):
                    raise DataValidationError(f"Invalid message data format for {msg_name}: must be a non-empty dictionary")
                
                fields = list(msg_data.keys())
                if not fields:
                    raise DataValidationError(f"No fields found for message {msg_name}")

                try:
                    num_rows = max([len(msg_data[field]) for field in fields])
                    if num_rows == 0:
                        logger.warning(f"No data rows found for message {msg_name}")
                        continue

                    rows = []
                    for i in range(num_rows):
                        try:
                            row = {}
                            for field in fields:
                                if i < len(msg_data[field]):
                                    value = msg_data[field][i]
                                    # Validate value type
                                    if value is not None and not isinstance(value, (int, float, str, bool, list)):
                                        raise DataValidationError(
                                            f"Invalid data type for field '{field}' in message {msg_name}: {type(value)}"
                                        )
                                    
                                    # Process the value using the new method
                                    processed_value = self._process_field_value(value, field, msg_name)
                                    row[field] = processed_value
                            rows.append(row)
                        except IndexError as e:
                            raise DataValidationError(
                                f"Data inconsistency in message {msg_name} at row {i}: {str(e)}"
                            )

                    if msg_name not in self.message_tables[session_id]:
                        try:
                            sample_row = rows[0]
                            self._create_table_for_message(session_id, msg_name, fields, sample_row)
                        except Exception as e:
                            raise DatabaseConnectionError(
                                f"Failed to create table for message {msg_name}: {str(e)}"
                            )

                    insert_fields = fields
                    placeholders = ", ".join(["?"] * len(insert_fields))
                    sql = f'INSERT INTO "{msg_name}" ({", ".join(insert_fields)}) VALUES ({placeholders})'
                    
                    for row in rows:
                        try:
                            values = [row.get(f) for f in fields]
                            conn.execute(sql, values)
                        except duckdb.Error as e:
                            logger.error(f"Failed to insert row into {msg_name}: {str(e)}")
                            raise DatabaseConnectionError(f"Failed to insert data: {str(e)}")

                    logger.debug(f"Successfully inserted rows into '{msg_name}'")
                except Exception as e:
                    logger.error(f"Error processing message {msg_name}: {str(e)}")
                    raise FlightDataDBError(f"Failed to process message {msg_name}: {str(e)}")

            logger.info(f"Successfully stored flight data for session {session_id}")
        except Exception as e:
            logger.error(f"Error storing flight data: {str(e)}")
            raise FlightDataDBError(f"Failed to store flight data: {str(e)}")

    def query(self, session_id: str, sql: str) -> pd.DataFrame:
        """
        Executes a SQL query on the database.

        Args:
            session_id (str): The session ID to execute the query on.
            sql (str): The SQL query to execute.

        Returns:
            pd.DataFrame: The result of the query.
        """
        if not session_id or not isinstance(session_id, str):
            raise DataValidationError("Invalid session_id: must be a non-empty string")
        if not sql or not isinstance(sql, str):
            raise DataValidationError("Invalid SQL query: must be a non-empty string")
        
        try:
            conn = self._get_connection(session_id)
            return conn.execute(sql).fetchdf()
        except duckdb.Error as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise DatabaseConnectionError(f"Failed to execute query: {str(e)}")
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise FlightDataDBError(f"Query execution failed: {str(e)}")

    def  get_database_information(self, session_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Gets the database information for a given session.

        Args:
            session_id (str): The session ID to get the database information for.
        """
        if not session_id or not isinstance(session_id, str):
            raise DataValidationError("Invalid session_id: must be a non-empty string")
        
        try:
            tables = self.message_tables[session_id]
            queries = [f"PRAGMA table_info('{table_name}')" for table_name in tables]
            results = {table_name: {"description": self._get_message_description(table_name), "schema": self.query(session_id, query)} for table_name, query in zip(tables, queries)}
            return results
        except Exception as e:
            logger.error(f"Error getting database information: {str(e)}")
            raise FlightDataDBError(f"Failed to get database information: {str(e)}")

    def close(self):
        """Close all database connections"""
        try:
            for session_id, conn in self.connections.items():
                try:
                    conn.close()
                    logger.debug(f"Closed connection for session {session_id}")
                except Exception as e:
                    logger.error(f"Error closing connection for session {session_id}: {str(e)}")
            self.connections.clear()
            self.message_tables.clear()
            logger.info("All database connections closed")
        except Exception as e:
            logger.error(f"Error during database cleanup: {str(e)}")
            raise FlightDataDBError(f"Failed to close database connections: {str(e)}")

    def _validate_and_clean_data(self, session_id: str, msg_name: str, fields: List[str], rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validates and cleans data to ensure compatibility with the database schema.
        
        Args:
            session_id (str): The session ID
            msg_name (str): The message name
            fields (List[str]): The field names
            rows (List[Dict[str, Any]]): The rows to validate and clean
            
        Returns:
            List[Dict[str, Any]]: The cleaned rows
        """
        try:
            if not rows:
                return rows
                
            # Get the table schema to understand expected types
            conn = self._get_connection(session_id)
            schema_query = f"PRAGMA table_info('{msg_name}')"
            schema_df = conn.execute(schema_query).fetchdf()
            
            # Create a mapping of field names to their expected types
            field_types = {}
            for _, row in schema_df.iterrows():
                field_name = str(row['name'])
                field_type = str(row['type']).upper()
                field_types[field_name] = field_type
            
            cleaned_rows = []
            for row in rows:
                cleaned_row = {}
                for field in fields:
                    value = row.get(field)
                    
                    if field in field_types:
                        expected_type = field_types[field]
                        
                        # Handle type conversion based on expected type
                        if expected_type in ('BIGINT', 'INTEGER'):
                            if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
                                # This is a JSON string that should be a number
                                try:
                                    parsed = json.loads(value)
                                    if isinstance(parsed, list) and len(parsed) > 0:
                                        if isinstance(parsed[0], list) and len(parsed[0]) > 0:
                                            value = parsed[0][0]  # Take first element of first array
                                        else:
                                            value = parsed[0]  # Take first element
                                except (json.JSONDecodeError, IndexError, TypeError):
                                    logger.warning(f"Could not parse JSON string for field {field}: {value}")
                                    value = None
                        
                        # Ensure the value is the correct type
                        if expected_type in ('BIGINT', 'INTEGER') and value is not None:
                            try:
                                if isinstance(value, (int, float, str)):
                                    value = int(float(value))
                                else:
                                    value = None
                            except (ValueError, TypeError):
                                logger.warning(f"Could not convert {value} to integer for field {field}")
                                value = None
                        elif expected_type == 'DOUBLE' and value is not None:
                            try:
                                if isinstance(value, (int, float, str)):
                                    value = float(value)
                                else:
                                    value = None
                            except (ValueError, TypeError):
                                logger.warning(f"Could not convert {value} to float for field {field}")
                                value = None
                    
                    cleaned_row[field] = value
                
                cleaned_rows.append(cleaned_row)
            
            return cleaned_rows
            
        except Exception as e:
            logger.error(f"Error validating and cleaning data for {msg_name}: {str(e)}")
            # Return original rows if validation fails
            return rows

    def cleanup_existing_data(self, session_id: str) -> None:
        """
        Cleans up existing data in the database to fix type mismatches.
        This is useful when existing data was stored with incorrect types.
        
        Args:
            session_id (str): The session ID to clean up
        """
        try:
            if session_id not in self.message_tables:
                logger.info(f"No tables found for session {session_id}")
                return
                
            conn = self._get_connection(session_id)
            tables = list(self.message_tables[session_id])
            
            for table_name in tables:
                try:
                    logger.info(f"Cleaning up table: {table_name}")
                    
                    # Get all data from the table
                    select_query = f'SELECT * FROM "{table_name}"'
                    df = conn.execute(select_query).fetchdf()
                    
                    if df.empty:
                        logger.info(f"Table {table_name} is empty, skipping cleanup")
                        continue
                    
                    # Get table schema
                    schema_query = f"PRAGMA table_info('{table_name}')"
                    schema_df = conn.execute(schema_query).fetchdf()
                    
                    # Create a new table with correct types
                    temp_table_name = f"{table_name}_temp"
                    
                    # Build new table schema
                    columns = []
                    for _, row in schema_df.iterrows():
                        field_name = str(row['name'])
                        field_type = str(row['type']).upper()
                        
                        # Ensure numeric fields are properly typed
                        if field_type in ('BIGINT', 'INTEGER'):
                            columns.append(f'"{field_name}" BIGINT')
                        elif field_type == 'DOUBLE':
                            columns.append(f'"{field_name}" DOUBLE')
                        else:
                            columns.append(f'"{field_name}" {field_type}')
                    
                    # Create temporary table
                    create_temp_sql = f'CREATE TABLE "{temp_table_name}" ({", ".join(columns)})'
                    conn.execute(create_temp_sql)
                    
                    # Clean and insert data
                    cleaned_rows = []
                    for _, row in df.iterrows():
                        cleaned_row = {}
                        for field_name in df.columns:
                            value = row[field_name]
                            
                            # Find the expected type for this field
                            field_type = None
                            for _, schema_row in schema_df.iterrows():
                                if str(schema_row['name']) == field_name:
                                    field_type = str(schema_row['type']).upper()
                                    break
                            
                            if field_type in ('BIGINT', 'INTEGER'):
                                if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
                                    try:
                                        parsed = json.loads(value)
                                        if isinstance(parsed, list) and len(parsed) > 0:
                                            if isinstance(parsed[0], list) and len(parsed[0]) > 0:
                                                value = parsed[0][0]
                                            else:
                                                value = parsed[0]
                                    except (json.JSONDecodeError, IndexError, TypeError):
                                        value = None
                                
                                if value is not None:
                                    try:
                                        value = int(float(str(value)))
                                    except (ValueError, TypeError):
                                        value = None
                            
                            cleaned_row[field_name] = value
                        
                        cleaned_rows.append(cleaned_row)
                    
                    # Insert cleaned data into temporary table
                    if cleaned_rows:
                        insert_fields = list(cleaned_rows[0].keys())
                        placeholders = ", ".join(["?"] * len(insert_fields))
                        insert_sql = f'INSERT INTO "{temp_table_name}" ({", ".join(insert_fields)}) VALUES ({placeholders})'
                        
                        for cleaned_row in cleaned_rows:
                            values = [cleaned_row.get(f) for f in insert_fields]
                            conn.execute(insert_sql, values)
                    
                    # Replace original table with cleaned table
                    conn.execute(f'DROP TABLE "{table_name}"')
                    conn.execute(f'ALTER TABLE "{temp_table_name}" RENAME TO "{table_name}"')
                    
                    logger.info(f"Successfully cleaned up table {table_name}")
                    
                except Exception as e:
                    logger.error(f"Error cleaning up table {table_name}: {str(e)}")
                    # Continue with other tables
                    continue
            
            logger.info(f"Completed cleanup for session {session_id}")
            
        except Exception as e:
            logger.error(f"Error during data cleanup: {str(e)}")
            raise FlightDataDBError(f"Failed to cleanup existing data: {str(e)}")
