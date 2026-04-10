"""Minimal KG builder template for Assignment 4.

Keep this contract unchanged:
- Graph: (Regulation)-[:HAS_ARTICLE]->(Article)-[:CONTAINS_RULE]->(Rule)
- Article: number, content, reg_name, category
- Rule: rule_id, type, action, result, art_ref, reg_name
- Fulltext indexes: article_content_idx, rule_idx
- SQLite file: ncu_regulations.db
"""

import os
import sqlite3
from typing import Any

from dotenv import load_dotenv
from neo4j import GraphDatabase

from llm_loader import load_local_llm, get_tokenizer, get_raw_pipeline, generate_text


# ========== 0) Initialization ==========
load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH = (
    os.getenv("NEO4J_USER", "neo4j"),
    os.getenv("NEO4J_PASSWORD", "password"),
)


def extract_entities(article_number: str, reg_name: str, content: str) -> dict[str, Any]:
    """Extract rules from article content using LLM."""
    prompt = f"""
You are an expert at extracting structured rules from university regulation text.

Given the article content below, extract all the specific rules or regulations mentioned.
Each rule should have:
- type: The category of the rule (e.g., "penalty", "requirement", "procedure", "limitation", "permission")
- action: What the student does or the condition (e.g., "forgetting student ID", "being late", "cheating")
- result: The consequence or outcome (e.g., "5 points deduction", "barred from exam", "zero score")

Return only a JSON array of rule objects. If no rules are found, return an empty array.

Article: {article_number}
Regulation: {reg_name}
Content: {content}

Output format: [{{"type": "penalty", "action": "forgetting student ID", "result": "5 points deduction"}}]
"""
    
    messages = [{"role": "user", "content": prompt}]
    try:
        response = generate_text(messages, max_new_tokens=500)
        # Try to parse JSON
        import json
        start = response.find('[')
        end = response.rfind(']') + 1
        if start != -1 and end != -1:
            json_str = response[start:end]
            rules = json.loads(json_str)
            if isinstance(rules, list):
                return {"rules": rules}
    except:
        pass
    
    return {"rules": []}


def build_fallback_rules(article_number: str, content: str) -> list[dict[str, str]]:
    """TODO(student, optional): add deterministic fallback rules."""
    return []


# SQLite tables used:
# - regulations(reg_id, name, category)
# - articles(reg_id, article_number, content)


def build_graph() -> None:
    """Build KG from SQLite into Neo4j using the fixed assignment schema."""
    sql_conn = sqlite3.connect("ncu_regulations.db")
    cursor = sql_conn.cursor()
    driver = GraphDatabase.driver(URI, auth=AUTH)

    # Optional: warm up local LLM
    load_local_llm()

    with driver.session() as session:
        # Fixed strategy: clear existing graph data before rebuilding.
        session.run("MATCH (n) DETACH DELETE n")

        # 1) Read regulations and create Regulation nodes.
        cursor.execute("SELECT reg_id, name, category FROM regulations")
        regulations = cursor.fetchall()
        reg_map: dict[int, tuple[str, str]] = {}

        for reg_id, name, category in regulations:
            reg_map[reg_id] = (name, category)
            session.run(
                "MERGE (r:Regulation {id:$rid}) SET r.name=$name, r.category=$cat",
                rid=reg_id,
                name=name,
                cat=category,
            )

        # 2) Read articles and create Article + HAS_ARTICLE.
        cursor.execute("SELECT reg_id, article_number, content FROM articles")
        articles = cursor.fetchall()

        for reg_id, article_number, content in articles:
            reg_name, reg_category = reg_map.get(reg_id, ("Unknown", "Unknown"))
            session.run(
                """
                MATCH (r:Regulation {id: $rid})
                CREATE (a:Article {
                    number:   $num,
                    content:  $content,
                    reg_name: $reg_name,
                    category: $reg_category
                })
                MERGE (r)-[:HAS_ARTICLE]->(a)
                """,
                rid=reg_id,
                num=article_number,
                content=content,
                reg_name=reg_name,
                reg_category=reg_category,
            )

        # 3) Create full-text index on Article content.
        session.run(
            """
            CREATE FULLTEXT INDEX article_content_idx IF NOT EXISTS
            FOR (a:Article) ON EACH [a.content]
            """
        )

        rule_counter = 0

        # Extract rules from articles
        cursor.execute("SELECT reg_id, article_number, content FROM articles")
        articles = cursor.fetchall()

        for reg_id, article_number, content in articles:
            reg_name, reg_category = reg_map.get(reg_id, ("Unknown", "Unknown"))
            entities = extract_entities(article_number, reg_name, content)
            rules = entities.get("rules", [])
            
            for rule in rules:
                if not rule.get("action") or not rule.get("result"):
                    continue  # Skip invalid rules
                
                rule_id = f"rule_{rule_counter}"
                rule_counter += 1
                
                session.run(
                    """
                    MATCH (a:Article {number: $art_num})
                    CREATE (r:Rule {
                        rule_id: $rule_id,
                        type: $type,
                        action: $action,
                        result: $result,
                        art_ref: $art_ref,
                        reg_name: $reg_name
                    })
                    MERGE (a)-[:CONTAINS_RULE]->(r)
                    """,
                    art_num=article_number,
                    rule_id=rule_id,
                    type=rule.get("type", "general"),
                    action=rule["action"],
                    result=rule["result"],
                    art_ref=article_number,
                    reg_name=reg_name,
                )

        # 4) Create full-text index on Rule fields.
        session.run(
            """
            CREATE FULLTEXT INDEX rule_idx IF NOT EXISTS
            FOR (r:Rule) ON EACH [r.action, r.result]
            """
        )

        # 5) Coverage audit (provided scaffold).
        coverage = session.run(
            """
            MATCH (a:Article)
            OPTIONAL MATCH (a)-[:CONTAINS_RULE]->(r:Rule)
            WITH a, count(r) AS rule_count
            RETURN count(a) AS total_articles,
                   sum(CASE WHEN rule_count > 0 THEN 1 ELSE 0 END) AS covered_articles,
                   sum(CASE WHEN rule_count = 0 THEN 1 ELSE 0 END) AS uncovered_articles
            """
        ).single()

        total_articles = int((coverage or {}).get("total_articles", 0) or 0)
        covered_articles = int((coverage or {}).get("covered_articles", 0) or 0)
        uncovered_articles = int((coverage or {}).get("uncovered_articles", 0) or 0)

        print(
            f"[Coverage] covered={covered_articles}/{total_articles}, "
            f"uncovered={uncovered_articles}"
        )

    driver.close()
    sql_conn.close()


if __name__ == "__main__":
    build_graph()
