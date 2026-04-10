# NCU Regulation KG-based QA System

## KG Construction Logic and Design Choices

The Knowledge Graph is built from PDF regulations parsed into SQLite articles. Each article's content is processed by an LLM (Qwen/Qwen2.5-3B-Instruct) to extract structured rules with type, action, and result. Rules are created as nodes linked to articles.

Design choices:

- Use LLM for rule extraction to handle varied text formats.
- Fallback to empty rules if extraction fails.
- Unique rule IDs for deduplication.

## KG Schema/Diagram

```
(:Regulation)-[:HAS_ARTICLE]->(:Article)-[:CONTAINS_RULE]->(:Rule)
```

- Regulation: id, name, category
- Article: number, content, reg_name, category
- Rule: rule_id, type, action, result, art_ref, reg_name

Fulltext indexes: article_content_idx on Article.content, rule_idx on Rule.action and Rule.result.

## Key Cypher Query Design and Retrieval Strategy

Typed query: Filters by rule type and contains checks on action/result.
Broad query: Fulltext search on rule_idx.

Retrieval: Run both queries, merge results, deduplicate, add article content from DB.

## Failure Analysis + Improvements Made

Initial extraction had low coverage due to LLM parsing issues. Improved prompt specificity for better JSON output. Added article content fallback for context.

Query accuracy improved by combining typed and broad strategies, ensuring high recall.
Before you begin, ensure you have the following installed:

- Python 3.11 (Strict requirement)

- Docker Desktop (Required to run the Neo4j database)

- Internet access for first-time HuggingFace model download (local model will be cached)

# ⚙️ Environment Setup

### 1. Database Setup (Neo4j via Docker)

You must run a local Neo4j instance using Docker. Run the following command in your terminal:

`docker run -d --name neo4j -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:latest`

Explanation of flags:

- -d: Runs the container in detached mode (background).

- -p 7474:7474: Exposes the web interface port (Browser).

- -p 7687:7687: Exposes the Bolt protocol port (Python connection).

- -e NEO4J_AUTH=...: Sets the username (neo4j) and password (password).

Verification: After running the command, check if the database is ready:

1. Open your browser and go to http://localhost:7474.

2. Login with user: neo4j and password: password.

### 2. Virtual Environment Setup

It is highly recommended to use a virtual environment to manage dependencies.

**For macOS / Linux:**

```
# Create virtual environment
python -m venv venv

# Activate environment
source venv/bin/activate
```

**For Windows:**

```
# Create virtual environment
python -m venv venv

# Activate environment
venv\Scripts\activate
```

### 3. Install Dependencies

`pip install -r requirements.txt`

# 📂 File Descriptions

- **source/:** Folder containing raw English PDF regulations
- **setup_data.py:** Parses PDFs using pdfplumber and Regex, cleans the text, and stores structured data into a local SQLite database
- **build_kg.py:** Reads from SQLite and executes Cypher queries to create nodes (Regulation, Article) and relationships (HAS_ARTICLE) in Neo4j.
- **query_system.py:** The interactive chatbot. It retrieves full regulation context and uses the LLM to generate answers.
- **auto_test.py:** Runs benchmark questions in test_data.json and uses an "LLM-as-a-Judge" to score your system (Pass/Fail).

# 🚀 Execution Order

**make sure you have already run neo4j in docker**
**run commands in this repository root folder**

1. `python setup_data.py`
2. `python build_kg.py`
3. (Not necessary)`python query_system.py`: Test your system manually to see if it answers correctly.
4. `python auto_test.py`: run the benchmark test

## Report

### KG Construction Logic and Design Choices

The Knowledge Graph is built from PDF regulations parsed into SQLite articles. Each article's content is processed by an LLM (Qwen/Qwen2.5-3B-Instruct) to extract structured rules with type, action, and result. Rules are created as nodes linked to articles.

Design choices:

- Use LLM for rule extraction to handle varied text formats.
- Fallback to empty rules if extraction fails.
- Unique rule IDs for deduplication.

### KG Schema/Diagram

```
(:Regulation)-[:HAS_ARTICLE]->(:Article)-[:CONTAINS_RULE]->(:Rule)
```

- Regulation: id, name, category
- Article: number, content, reg_name, category
- Rule: rule_id, type, action, result, art_ref, reg_name

Fulltext indexes: article_content_idx on Article.content, rule_idx on Rule.action and Rule.result.

### Key Cypher Query Design and Retrieval Strategy

Typed query: Filters by rule type and contains checks on action/result.
Broad query: Fulltext search on rule_idx.

Retrieval: Run both queries, merge results, deduplicate, add article content from DB.

### Failure Analysis + Improvements Made

Initial extraction had low coverage due to LLM parsing issues. Improved prompt specificity for better JSON output. Added article content fallback for context.

Query accuracy improved by combining typed and broad strategies, ensuring high recall.
