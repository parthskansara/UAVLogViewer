from typing import Dict, List, Any, Optional
from openai import OpenAI
import json
from datetime import datetime
import os
from dotenv import load_dotenv
import logging
from tools.flight_data_db import FlightDataDB
from tools.sql_tools import SQLTools
from models import Message, FlightData, AgentResponse
from openai.types.chat import ChatCompletionMessageParam


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)

logging.getLogger("openai").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

class SQLQueryAgent:
    """Agent responsible for converting natural language questions to SQL queries and executing them safely."""
    
    def __init__(self):
        self.sql_tools = SQLTools()
        logger.info("Initializing SQLQueryAgent with system prompt for SQL query generation")
        self.system_prompt = """You are a SQL query generation expert for UAV flight data analysis. Your role is to convert natural language questions into SQL queries by understanding the database schema.
        
        Guidelines:
        1. Do not ask for clarification. Make logical assumptions about the user's intent and generate the SQL query accordingly.
        2. Assume that the user is asking about the entire flight duration unless they specify otherwise.
        3. Only generate SELECT queries - never generate INSERT, UPDATE, DELETE, or DROP statements
        4. Use proper SQL syntax and table aliases for clarity
        5. Include appropriate JOINs when querying across multiple tables
        6. Use aggregate functions (COUNT, AVG, MAX, MIN) when appropriate
        7. Format timestamps and numeric values appropriately
        6. If the question is unclear, ask for clarification about:
           - Time period of interest
           - Specific metrics or parameters
           - Any filtering conditions
           - Desired level of detail in the response
        7. Wrap the snippet in a single ```sql code fence (no extra prose).
        
        Example questions and SQL queries:
        
        Q: "What was the average altitude during the flight?"
        A: SELECT AVG(altitude) as avg_altitude FROM ATTITUDE;
        
        Q: "Show me the battery status over time"
        A: SELECT time_boot_ms, battery_remaining, current_battery 
           FROM BATTERY_STATUS 
           ORDER BY time_boot_ms;
        
        Q: "What was the maximum speed reached?"
        A: SELECT MAX(airspeed) as max_speed 
           FROM VFR_HUD;

        
        Remember to validate the query before execution and ensure it only reads data."""
        
    
    def _generate_answer(self, question: str, query_results: Any, conversation_history: List[Message], db_schema: Dict[str, Any]) -> str:
        """Generate natural language answer from query results."""
        logger.info(f"Starting answer generation for question: {question}")
        logger.debug(f"Query results type: {type(query_results)}")

        
        try:
            # Convert DataFrame to JSON-serializable format
            if hasattr(query_results, 'to_dict'):
                # Handle pandas DataFrame
                serializable_results = query_results.to_dict(orient='records')
            else:
                # Handle other types (lists, dicts, etc.)
                serializable_results = query_results

            prompt = f"""
            You are a UAV flight data expert. Generate clear, concise answers based on SQL query results. 
            
            Ensure to use the correct units as specified in the table description below. 

            Convert the result to the metric unit system (m, km/h, etc.) wherever applicable.
            
            Table descriptions: \n{db_schema}

            Original question: {question}
            Query results: {json.dumps(serializable_results, indent=2)}

            
            """

            messages: List[ChatCompletionMessageParam] = [
                {"role": "system", "content": prompt},
                *[msg.to_openai_message() for msg in conversation_history],
                {"role": "user", "content": f"Original question: {question}\nQuery results:\n{json.dumps(serializable_results, indent=2)}"}
            ]
            
            logger.debug("Preparing OpenAI API request for answer generation")
            logger.debug(f"Number of messages in conversation history: {len(conversation_history)}")
            
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                temperature=0.7,
                max_tokens=1024
            )
            
            answer = response.choices[0].message.content
            if answer is None:
                raise Exception("No content received from OpenAI API")
            answer = answer.strip()
            logger.info("Successfully generated answer")
            logger.debug(f"Generated answer length: {len(answer)} characters")
            return answer
        except Exception as e:
            logger.error(f"Error generating answer: {str(e)}", exc_info=True)
            raise Exception(f"Error generating answer: {str(e)}")
    
    def _needs_clarification(self, question: str, schema: Dict[str, Any], conversation_history: List[Message]) -> Optional[str]:
        """Determine if the question needs clarification and return clarifying question if needed."""
        logger.info(f"Checking if question needs clarification: {question}")
        logger.debug(f"Available tables in schema: {list(schema.keys())}")
        
        try:
            messages: List[ChatCompletionMessageParam] = [
                {"role": "system", "content": """You are a SQL query expert. Your role is to determine if a question needs clarification before generating a SQL query.
                
                    Guidelines for determining if clarification is needed:
                    1. ONLY ask for clarification if the question is ambiguous or cannot be answered with reasonable defaults.
                    2. If the user does not specify a time period, assume the entire flight duration.
                    3. ANY question that is not about flight data analysis needs clarification
                    4. ANY greeting or casual conversation needs clarification
                    5. ANY question that doesn't mention specific flight parameters needs clarification
                    6. ANY question that doesn't specify what data to analyze needs clarification
                    7. ANY question that asks about flight parameters that are not in the database schema needs clarification

                    Valid questions are those that:
                    - Ask about specific flight parameters (eg: altitude, speed, battery, etc.)
                    - Request analysis of flight data
                    - Ask for statistics or trends in the data
                    - Ask for specific events or parameters like when did the altitude cross a certain value, or when did the velocity exceed a certain value
                    - Compare different flight parameters
                    - If flight period is not specified, assume the entire flight duration

                    Example clarifying questions:
                    - "Which specific metrics would you like to see?"
                    - "Do you want to see data for a specific flight mode?"
                    - "Would you like to see the data aggregated or as a time series?"
                    - "What specific flight parameters would you like to analyze?"


                    Examples of questions that do not need clarification:
                    - Q: "What is the total flight duration?"
                      A: null (No clarification needed; assume entire flight duration)
                    - Q: "Show me the average altitude."
                      A: null (No clarification needed; assume entire flight duration)
                    - Q: "When did the altitude cross 100 meters?"
                      A: null (No clarification needed; assume entire flight duration)
                    - Q: "When did the velocity exceed 10 m/s?"
                      A: null (No clarification needed; assume entire flight duration)

                    If a question asks about a parameter that is not in the database schema, return "This parameter is not in the flight data, please ask a different question."

                    If a question has been asked before in the conversation history, mention that it was asked before and return the previous answer.
                    
                    If clarification is needed, return only the specific clarifying question to ask the user.
                    If no clarification is needed, return "null".
                    """},
                {"role": "system", "content": f"List of tables: {schema.keys()}\n\nDatabase schema as generated by the query 'PRAGMA table_info(table_name)' for each table:\n{schema}"},
                *[msg.to_openai_message() for msg in conversation_history],
                {"role": "user", "content": f"Question: {question}\n\nDoes this question need clarification? If yes, what specific question should I ask the user?"}
            ]
            
            logger.debug("Preparing OpenAI API request for clarification check")
            logger.debug(f"Number of messages in conversation history: {len(conversation_history)}")
            
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                temperature=0.3,
                max_tokens=100
            )
            
            clarification = response.choices[0].message.content
            if clarification is None:
                raise Exception("No content received from OpenAI API")
            clarification = clarification.strip()
            logger.info(f"Clarification response: {clarification}")
            needs_clarification = clarification.lower() not in ['null', 'none', 'no clarification needed']
            
            if needs_clarification:
                logger.info(f"Clarification needed: {clarification}")
            else:
                logger.info("No clarification needed")
                
            return None if not needs_clarification else clarification
            
        except Exception as e:
            logger.error(f"Error checking for clarification: {str(e)}", exc_info=True)
            return None

    def process_question(self, session_id: str, question: str, schema: Dict[str, Any], conversation_history: List[Message], flight_db: FlightDataDB) -> str:
        """Process a natural language question and return an answer."""
        logger.info(f"Starting question processing for session {session_id}: {question}")
        logger.debug(f"Available tables in schema: {list(schema.keys())}")
        logger.debug(f"Number of messages in conversation history: {len(conversation_history)}")
        
        try:
            # Check if clarification is needed
            clarification = self._needs_clarification(question, schema, conversation_history)
            if clarification:
                logger.info(f"Returning clarification request: {clarification}")
                return f"{clarification}"

            user_prompt = f"\nQuestion: {question}"
            sql_query = self.sql_tools.generate_sql_query(self.system_prompt, user_prompt, question, schema, conversation_history)
    
            # Execute query
            logger.info(f"Executing SQL query: {sql_query}")
            query_results = flight_db.query(session_id, sql_query)
            logger.debug(f"Query execution completed. Results: {query_results}")
            
            # Generate answer
            answer = self._generate_answer(question, query_results, conversation_history, schema)
            logger.info("Successfully processed question")
            return answer
            
        except Exception as e:
            logger.error(f"Error processing question: {str(e)}", exc_info=True)
            return f"Error processing question: {str(e)}"