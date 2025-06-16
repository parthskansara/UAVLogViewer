# UAV Log Viewer

A web application for viewing and analyzing UAV flight logs with an AI-powered chatbot interface.

## Features

- Interactive 3D visualization of flight paths
- Real-time parameter monitoring
- AI-powered chatbot for data analysis
- SQL and statistical analysis capabilities
- Event logging and analysis

## Chatbot Integration

### Overview
This feature introduces an intelligent chatbot feature named "Maverick" that provides natural language interaction for analyzing UAV flight data. The chatbot is designed to help users understand and analyze flight logs through conversational queries.

### Frontend Implementation
- Added a new `ChatBot.vue` component with a modern, responsive UI
- Features a collapsible chat interface with minimize/maximize functionality
- Real-time message updates with smooth animations
- Loading states and error handling for user interactions
- Integration with the backend API through a dedicated chat service

### Backend Architecture
The backend implements a sophisticated multi-agent architecture for processing user queries:

**1. Agent Orchestrator**
- Central coordinator that manages the flow of queries between different specialized agents
- Maintains conversation history for context-aware responses
- Handles session management and database connections

**2. Query Classifier Agent**
- First point of contact for incoming queries
- Classifies queries into three categories:
  - SQL: For direct data retrieval and simple aggregations
  - ANALYSIS: For complex data analysis and pattern recognition
  - NONE: For queries unrelated to flight data

**3. SQL Query Agent**
- Handles direct data retrieval queries
- Converts natural language to SQL queries
- Implements safety checks to prevent malicious queries
- Features:
  - Query validation and sanitization
  - Context-aware query generation
  - Clarification requests for ambiguous queries
  - Natural language response generation from query results

**4. Data Analysis Agent**
Comprises three sub-agents:
- **Data Extraction Agent**: Generates and executes SQL queries to gather relevant data
- **Code Generation Agent**: Creates Python code for data analysis using pandas and scikit-learn
- **Reasoning Agent**: Provides expert analysis and insights based on the results

### API Endpoints
Added new endpoints:
- `POST /api/chat`: Main endpoint for processing chat messages
  - Accepts: message, sessionId (optional), flightData (optional)
  - Returns: message, sessionId, error (optional)

### Technical Details
- Implemented using FastAPI for the backend
- Uses OpenAI's GPT-4o-mini for natural language processing
- Integrates with DuckDB for efficient data storage and querying
- Implements comprehensive error handling and logging
- Maintains conversation context for improved response quality

### Security Features
- Query validation to prevent SQL injection
- Read-only database operations
- Session-based conversation management
- Secure API key handling through environment variables

## Prerequisites

- Node.js (v14 or higher)
- Python 3.8 or higher
- npm or yarn package manager

## Installation

1. Clone the repository:
```bash
git clone https://github.com/parthskansara/UAVLogViewer.git
cd UAVLogViewer
```

2. Install backend dependencies:
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
pip install -r requirements.txt
```

3. Install frontend dependencies:
```bash
cd ../frontend
npm install
```

## Running the Application

### Backend Server

1. Navigate to the backend directory:
```bash
cd backend
```

2. Activate the virtual environment (if not already activated):
```bash
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

3. Start the backend server:
```bash
# For production:
python app.py

# For development with hot reloading:
uvicorn app:app --reload --host localhost --port 5000
```
The backend server will start running on `http://localhost:5000`

### Frontend Client

1. Navigate to the frontend directory:
```bash
cd frontend
```

2. Start the development server:
```bash
npm run dev
```
The frontend application will start running on `http://localhost:8080`

## Development

- Backend API documentation is available at `http://localhost:5000/api/docs`
- Frontend development server includes hot-reloading for instant feedback
- Backend server will automatically reload when changes are detected using uvicorn




