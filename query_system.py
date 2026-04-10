"""Minimal KG query template for Assignment 4.

Keep these APIs unchanged for auto-test:
- generate_text(messages, max_new_tokens=220)
- get_relevant_articles(question)
- generate_answer(question, rule_results)

Keep Rule fields aligned with build_kg output:
rule_id, type, action, result, art_ref, reg_name
"""

import os
from typing import Any

from neo4j import GraphDatabase
from dotenv import load_dotenv

from llm_loader import load_local_llm, get_tokenizer, get_raw_pipeline


# ========== 0) Initialization ==========
load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH = (
	os.getenv("NEO4J_USER", "neo4j"),
	os.getenv("NEO4J_PASSWORD", "password"),
)

# Avoid local proxy settings interfering with model/Neo4j access.
for key in ["http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
	if key in os.environ:
		del os.environ[key]


try:
	driver = GraphDatabase.driver(URI, auth=AUTH)
	driver.verify_connectivity()
except Exception as e:
	print(f"⚠️ Neo4j connection warning: {e}")
	driver = None


# ========== 1) Public API (query flow order) ==========
# Order: extract_entities -> build_typed_cypher -> get_relevant_articles -> generate_answer

def generate_text(messages: list[dict[str, str]], max_new_tokens: int = 220) -> str:
	"""
	Call local HF model via chat template + raw pipeline.

	Interface:
	- Input:
	  - messages: list[dict[str, str]] (chat messages with role/content)
	  - max_new_tokens: int
	- Output:
	  - str (model generated text, no JSON guarantee)
	"""
	tok = get_tokenizer()
	pipe = get_raw_pipeline()
	if tok is None or pipe is None:
		load_local_llm()
		tok = get_tokenizer()
		pipe = get_raw_pipeline()
	prompt = tok.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
	return pipe(prompt, max_new_tokens=max_new_tokens)[0]["generated_text"].strip()


def extract_entities(question: str) -> dict[str, Any]:
    """Parse question to extract type, terms, aspect."""
    prompt = f"""
Analyze this question about university regulations and extract:
- question_type: The type of question (e.g., "penalty", "requirement", "procedure", "fee", "duration", "score", "general")
- subject_terms: Key terms related to the subject (e.g., ["student ID", "exam"])
- aspect: The specific aspect or condition (e.g., "forgetting", "late", "cheating")

Return only JSON.

Question: {question}

Output: {{"question_type": "penalty", "subject_terms": ["student ID"], "aspect": "forgetting"}}
"""
    
    messages = [{"role": "user", "content": prompt}]
    try:
        response = generate_text(messages, max_new_tokens=150)
        import json
        start = response.find('{')
        end = response.rfind('}') + 1
        if start != -1 and end != -1:
            json_str = response[start:end]
            entities = json.loads(json_str)
            return entities
    except:
        pass
    
    # Fallback
    return {
        "question_type": "general",
        "subject_terms": [],
        "aspect": "general",
    }


def build_typed_cypher(entities: dict[str, Any]) -> tuple[str, str]:
    """Return (typed_query, broad_query) with score and required fields."""
    q_type = entities.get("question_type", "general")
    terms = entities.get("subject_terms", [])
    aspect = entities.get("aspect", "general")
    
    # Typed query: specific to type and terms
    conditions = []
    if q_type != "general":
        conditions.append(f"r.type = '{q_type}'")
    if terms:
        term_conditions = " OR ".join([f"r.action CONTAINS '{term}' OR r.result CONTAINS '{term}'" for term in terms])
        conditions.append(f"({term_conditions})")
    if aspect != "general":
        conditions.append(f"r.action CONTAINS '{aspect}'")
    
    where_clause = " AND ".join(conditions) if conditions else "true"
    
    cypher_typed = f"""
    MATCH (r:Rule)
    WHERE {where_clause}
    RETURN r.rule_id, r.type, r.action, r.result, r.art_ref, r.reg_name, 1.0 as score
    ORDER BY score DESC
    LIMIT 10
    """
    
    # Broad query: fulltext search
    search_terms = " ".join(terms + [aspect] + [q_type])
    cypher_broad = f"""
    CALL db.index.fulltext.queryNodes("rule_idx", "{search_terms}") YIELD node, score
    RETURN node.rule_id, node.type, node.action, node.result, node.art_ref, node.reg_name, score
    ORDER BY score DESC
    LIMIT 10
    """
    
    return cypher_typed, cypher_broad


def get_relevant_articles(question: str) -> list[dict[str, Any]]:
    """Run typed+broad retrieval and return merged rule dicts with article content."""
    if driver is None:
        return []
    
    entities = extract_entities(question)
    typed_query, broad_query = build_typed_cypher(entities)
    
    results = []
    seen = set()
    
    with driver.session() as session:
        # Run typed query
        try:
            typed_results = session.run(typed_query)
            for record in typed_results:
                rule_id = record["r.rule_id"]
                if rule_id not in seen:
                    seen.add(rule_id)
                    rule = {
                        "rule_id": rule_id,
                        "type": record["r.type"],
                        "action": record["r.action"],
                        "result": record["r.result"],
                        "art_ref": record["r.art_ref"],
                        "reg_name": record["r.reg_name"],
                        "score": record["score"],
                        "source": "typed"
                    }
                    results.append(rule)
        except:
            pass
        
        # Run broad query
        try:
            broad_results = session.run(broad_query)
            for record in broad_results:
                rule_id = record["node.rule_id"]
                if rule_id not in seen:
                    seen.add(rule_id)
                    rule = {
                        "rule_id": rule_id,
                        "type": record["node.type"],
                        "action": record["node.action"],
                        "result": record["node.result"],
                        "art_ref": record["node.art_ref"],
                        "reg_name": record["node.reg_name"],
                        "score": record["score"],
                        "source": "broad"
                    }
                    results.append(rule)
        except:
            pass
    
    # Add article content from DB
    import sqlite3
    conn = sqlite3.connect("ncu_regulations.db")
    cursor = conn.cursor()
    
    for rule in results:
        art_ref = rule["art_ref"]
        cursor.execute("SELECT content FROM articles WHERE article_number = ?", (art_ref,))
        row = cursor.fetchone()
        if row:
            rule["article_content"] = row[0]
        else:
            rule["article_content"] = ""
    
    conn.close()
    
    return results


def generate_answer(question: str, rule_results: list[dict[str, Any]]) -> str:
    """Generate grounded answer from retrieved rules only."""
    if not rule_results:
        return "Insufficient rule evidence to answer this question."
    
    # Prepare context
    context = "Relevant rules:\n"
    for rule in rule_results[:5]:  # Limit to top 5
        context += f"- Type: {rule['type']}\n"
        context += f"  Action: {rule['action']}\n"
        context += f"  Result: {rule['result']}\n"
        context += f"  Source: {rule['art_ref']} ({rule['reg_name']})\n"
        if rule.get('article_content'):
            # Add snippet
            content = rule['article_content']
            if len(content) > 200:
                content = content[:200] + "..."
            context += f"  Article text: {content}\n"
        context += "\n"
    
    prompt = f"""
Based on the following retrieved rules and article content, answer the question directly and concisely.
Cite the source article in your answer.

Question: {question}

{context}

Answer format: Direct answer. [Source: Article X]
"""
    
    messages = [{"role": "user", "content": prompt}]
    try:
        answer = generate_text(messages, max_new_tokens=150)
        return answer.strip()
    except:
        return "Unable to generate answer from retrieved evidence."


def main() -> None:
	"""Interactive CLI (provided scaffold)."""
	if driver is None:
		return

	load_local_llm()

	print("=" * 50)
	print("🎓 NCU Regulation Assistant (Template)")
	print("=" * 50)
	print("💡 Try: 'What is the penalty for forgetting student ID?'")
	print("👉 Type 'exit' to quit.\n")

	while True:
		try:
			user_q = input("\nUser: ").strip()
			if not user_q:
				continue
			if user_q.lower() in {"exit", "quit"}:
				print("👋 Bye!")
				break

			results = get_relevant_articles(user_q)
			answer = generate_answer(user_q, results)
			print(f"Bot: {answer}")

		except KeyboardInterrupt:
			print("\n👋 Bye!")
			break
		except NotImplementedError as e:
			print(f"⚠️ {e}")
			break
		except Exception as e:
			print(f"❌ Error: {e}")

	driver.close()


if __name__ == "__main__":
	main()

