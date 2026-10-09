# SQL-cached Jev decisions through Vercel AI Gateway

Jev evaluates a shared **state** against named, typed **questions**. It returns
decisions and probabilities, not generated prose. Use
`JevPromptResponseCacheSQL` for this API; the general `PromptResponseCacheSQL`
uses the Responses API and cannot send Jev's request format.

This adapter calls Vercel's TypeSafe-compatible `/typesafe/v1/systemone`
endpoint. Set `AI_GATEWAY_API_KEY`, or configure `[vercel] vercel_api_key` as in
the [gateway tutorial](VERCEL_AI_GATEWAY_TUTORIAL.md). The existing PostgreSQL
`prompt` and `prompt_response` tables are required.

```python
import json

from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_jev import JevPromptResponseCacheSQL

questions = {
    "group": {
        "type": "choice",
        "instructions": (
            "Which broad industry does this company operate in? "
            "Judge the company's own operations as this filing describes them."
        ),
        "criteria": {
            "01": "Agricultural production — crops",
            "60": "Depository institutions — banks and credit unions",
            # Add the rest of your taxonomy here.
        },
    }
}

with get_session() as session:
    cache = JevPromptResponseCacheSQL(session=session, questions=questions)
    row = cache.get_cache_or_run(
        given_id="filing-123",
        text="The company provides commercial banking services.",  # Jev's state
    )

if row is None:
    raise RuntimeError("Jev request failed; inspect the logged error")
answer = json.loads(row["response_text"])["group"]
print(answer["choice"], answer["confidence"])
```

`response_text` is JSON containing the named `answers`; `response_full` holds
the complete gateway response as JSON. The cache hashes the complete question
definition as its prompt identity and the state text as its input identity, so
editing criteria or instructions causes a cache miss. It sets
`filter_by_model=True` to keep Jev rows separate from other model results.

Bulk calls use the same shape:

```python
filings = [
    {
        "id": "bank-001",
        "text": "Harbor Bank accepts deposits and makes commercial loans.",
    },
    {
        "id": "farm-002",
        "text": "Valley Orchard grows apples and sells the harvested fruit.",
    },
]

with get_session() as session:
    cache = JevPromptResponseCacheSQL(session=session, questions=questions)
    rows = cache.bulk_get_cache_or_run(
        [(filing["id"], filing["text"]) for filing in filings],
        max_workers=4,
    )
    for filing_id, row in rows.items():
        answer = json.loads(row["response_text"])["group"]
        print(filing_id, answer["choice"], answer["confidence"])
```

## More cookbook patterns

The examples below adapt TypeSafe's other cookbooks to this SQL cache. Those
cookbooks pass structured state directly to the TypeSafe client. Here, encode
structured state as a stable JSON string because the cache stores input in a
text column. The examples reuse the `json` and `get_session` imports above and
provide all of their input values. Each call opens a database session.
The output blocks below were captured from live Vercel AI Gateway calls on
October 9, 2026, with these fictional inputs. Those demonstration calls did
not write records to the SQL cache; probabilities can change with the model.

```python
def evaluate(session, item_id, state, questions):
    cache = JevPromptResponseCacheSQL(session=session, questions=questions)
    row = cache.get_cache_or_run(
        given_id=item_id,
        text=json.dumps(state, sort_keys=True),
    )
    if row is None:
        raise RuntimeError(f"Jev failed for {item_id}")
    return json.loads(row["response_text"])
```

### Screen a retrieved passage

The [RAG passage cookbook](https://docs.typesafe.ai/cookbooks/classifying_rag_passages)
asks several `Noul` questions about the query and one passage in a single
request. Each `noul` answer is the probability of “yes”. The application sets
its own thresholds and decides which passages reach the answering model.

```python
passage_questions = {
    "relevant": {
        "type": "noul",
        "instructions": "Does this passage address the query?",
    },
    "answer_evidence": {
        "type": "noul",
        "instructions": "Does it provide evidence usable in a direct answer?",
    },
    "prompt_injection": {
        "type": "noul",
        "instructions": "Does it try to instruct the answering model?",
    },
}

query_id = "grant-policy-001"
query = "Can this grant pay for general operating expenses?"
passage = {
    "id": "grant-guidelines-p4",
    "title": "Grant guidelines: eligible costs",
    "source_type": "official_guidelines",
    "text": (
        "The Harbor Green Fund provides unrestricted grants. Recipients may "
        "use the funds for salaries, rent, and other general operating costs."
    ),
}

with get_session() as session:
    answers = evaluate(
        session,
        f"{query_id}:{passage['id']}",
        {"query": query, "passage": passage},
        passage_questions,
    )

if answers["prompt_injection"]["noul"] >= 0.8:
    route = "drop"
elif answers["relevant"]["noul"] < 0.7:
    route = "drop"
elif answers["answer_evidence"]["noul"] >= 0.7:
    route = "evidence"
else:
    route = "review"
print("passage route:", route)
print("passage answers:", json.dumps(answers, indent=2, sort_keys=True))
```

Captured output:

```text
passage route: evidence
passage answers: {
  "answer_evidence": {
    "noul": 0.99,
    "type": "noul"
  },
  "prompt_injection": {
    "noul": 0.32,
    "type": "noul"
  },
  "relevant": {
    "noul": 0.99,
    "type": "noul"
  }
}
```

### Check whether a citation supports a claim

The [citation cookbook](https://docs.typesafe.ai/cookbooks/citation_check)
first checks whether a quoted span exists in the source. For quotes that do,
one `Choice` question judges the claim against the surrounding section.

```python
citation_id = "grant-deadline-001"
claim = "Applications close on June 30, 2026."
quote = "Applications close on June 30, 2026."
source_section = (
    "Applications open on April 1, 2026. "
    "Applications close on June 30, 2026. "
    "Late submissions will not be reviewed."
)

needs_review = False
if quote not in source_section:
    verdict = "fabricated"
else:
    citation_questions = {
        "relation": {
            "type": "choice",
            "instructions": "How does the section relate to the claim?",
            "criteria": {
                "supports": "It states or directly implies the claim.",
                "contradicts": "It states or implies the opposite.",
                "says_nothing": "It does not address the claim.",
            },
        }
    }
    with get_session() as session:
        answer = evaluate(
            session,
            citation_id,
            {"claim": claim, "section": source_section},
            citation_questions,
        )["relation"]
    verdict = answer["choice"]
    needs_review = answer["confidence"] < 0.8
print("citation verdict:", verdict, "review:", needs_review)
if quote in source_section:
    print("citation answer:", json.dumps(answer, indent=2, sort_keys=True))
```

Captured output:

```text
citation verdict: supports review: False
citation answer: {
  "choice": "supports",
  "confidence": 1,
  "probabilities": {
    "contradicts": 0,
    "says_nothing": 0,
    "supports": 1
  },
  "type": "choice"
}
```

### Match records from two sources

The [entity alignment cookbook](https://docs.typesafe.ai/cookbooks/entity_alignment)
combines an ordered `Score` with companion `Noul` questions that show which
fields agree. `score` is a value along the 0–2 scale defined by the three
criteria; `confidence` is a separate measure of certainty.

```python
match_questions = {
    "match": {
        "type": "score",
        "instructions": "Do these records describe the same organization?",
        "criteria": [
            "Different organizations.",
            "Possibly related; a person should review.",
            "The same organization.",
        ],
    },
    "same_name": {
        "type": "noul",
        "instructions": "Do the names refer to the same organization?",
    },
    "same_location": {
        "type": "noul",
        "instructions": "Do the locations agree?",
    },
}

pair_id = "harbor-green-fund-pair-001"
record_a = {
    "name": "Harbor Green Fund",
    "city": "Oakland",
    "state": "CA",
    "website": "https://harborgreen.example.org",
    "description": "Grants for neighborhood climate projects",
}
record_b = {
    "name": "Harbor Green Fund, Inc.",
    "city": "Oakland",
    "state": "California",
    "website": "https://harborgreen.example.org",
    "description": "Funds community climate and resilience work",
}

with get_session() as session:
    answers = evaluate(
        session,
        pair_id,
        {"record_a": record_a, "record_b": record_b},
        match_questions,
    )

score = answers["match"]["score"]
confidence = answers["match"]["confidence"]
name_probability = answers["same_name"]["noul"]
outcome = ["different", "review", "same"][min(int(score + 0.5), 2)]
if confidence < 0.8:
    outcome = "review"
print("entity outcome:", outcome)
print("name match probability:", name_probability)
print("entity answers:", json.dumps(answers, indent=2, sort_keys=True))
```

Captured output:

```text
entity outcome: same
name match probability: 0.93
entity answers: {
  "match": {
    "confidence": 0.98,
    "legend": {
      "0": "Different organizations.",
      "1": "Possibly related; a person should review.",
      "2": "The same organization."
    },
    "probabilities": {
      "0": 0,
      "1": 0.01,
      "2": 0.99
    },
    "score": 1.99,
    "type": "score"
  },
  "same_location": {
    "noul": 0.94,
    "type": "noul"
  },
  "same_name": {
    "noul": 0.93,
    "type": "noul"
  }
}
```

The [parallel questions cookbook](https://docs.typesafe.ai/cookbooks/parallel_questions)
uses the same pattern: put multiple questions about one state in one mapping,
so the state is sent once. The thresholds above are illustrative; calibrate
them against labeled examples from your own data.

The `questions` mapping follows [TypeSafe's Choice format](https://docs.typesafe.ai/cookbooks/classification_using_confidence),
including `instructions` and `criteria`. Vercel uses the model ID
`typesafe-ai/jev` for this route. For a yes/no question, use TypeSafe's `noul`
question type; its answer contains the probability in `noul`. See
[Vercel's TypeSafe API guide](https://vercel.com/docs/ai-gateway/sdks-and-apis/typesafe).

The repository pins `openai<2`, so its OpenAI SDK does not provide the newer
`decisions.create()` method. This adapter uses the already installed `requests`
library and the repository's existing gateway credential loader.

The adapter retries connection failures, timeouts, and HTTP 429/500/502/503/504
responses twice by default, with one- and two-second waits. Set
`max_retries=0` on `JevPromptResponseCacheSQL` to disable these retries. If all
attempts fail, the existing cache records one error for that call; use the bulk
method's `max_errors` option to allow later runs to retry that record.
