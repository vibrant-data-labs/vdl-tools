from vdl_tools.shared_tools.openai.openai_api_utils import CLIENT
import openai, json
from openai import OpenAI
from pydantic import BaseModel
import pandas as pd

client = OpenAI()
client = CLIENT


class SummaryFields(BaseModel):
  summary: str
  technologies: str
  products: str
  hq: str
  communities: str

class OrgSummaryItem(BaseModel):
  scraped_text: str        # The raw website content
  generated_summary: SummaryFields   # The model-generated JSON summary string
  link: str                # The URL of the website




system_prompt = """
You are **Website-Summary Grader**, an expert technical writer.  
Your job is to score how well *Model A* created a summirized description of an organization based on the text of their website.

### What you receive
1. **Website Text** – the full text of taken from the organization's website.
2. **Candidate explanation** – the answer produced by Model A that tries to describe what the organization does.

### What to produce
Return a single JSON object that can be parsed by `json.loads`, containing:
```json
{
  "accuracy": {"score": 5, "comment": "All claims are directly traceable to the source."},
  "completeness": {"score": 4, "comment": "Minor omission of one product mentioned."},
  ...
  "result": "float"
}
```

### Scoring dimensions (evaluate in this order)
1. Accuracy 45%: Are all claims in the summary directly supported by the website text, without inference or hallucination?
2. Completeness 30%: Does the summary capture all key points from the source? Including:
  - Technologies used by organization to address an issue
  - Main Mission and vision of the organization
  - Products, services and solutions offered
  - Communities the organization engages with
  - Programs tailored towards climate equity and justice
3. Objectivity 15%: Is the tone neutral and factual, not promotional?
4. Clarity 10%: Is the language clear, concise, and well-structured?

For each dimension, assign a 'score' from 1 to 5 (inclusive) and provide a short 'comment' justiying your score.
Use the full scale.
Then set "result": to the weighted average of scores
Be rigorous and unbiased.

"""


def get_delta(row, metric):
    """
    Calculate the delta for a given metric between new and old scores.
    """
    print(row)
    new_score = row[f"{metric}_new"]["score"]
    old_score = row[f"{metric}_old"]["score"]
    return new_score - old_score

def all_deltas(df):
  print(df.columns)
  df["delta_accuracy"] = df.apply(get_delta, axis=1, args=("accuracy",))
  df["delta_completeness"] = df.apply(get_delta, axis=1, args=("completeness",))
  df["delta_objectivity"] = df.apply(get_delta, axis=1, args=("objectivity",))
  df["delta_clarity"] = df.apply(get_delta, axis=1, args=("clarity",))


  # SUmmarize the deltas
  for col in ["delta_accuracy", "delta_completeness", "delta_objectivity", "delta_clarity"]:
    print("="*30)
    print(col)   
    print(df[col].describe())

#COMPARE
df_new = pd.read_json("C:\\Users\\naims\\Documents\\vdl\\eval_for_new2.json") 
df_old = pd.read_json("C:\\Users\\naims\\Documents\\vdl\\eval_for_old2.json") 

# join on uuid
df = df_new.merge(df_old, on="url_homepage", suffixes=("_new", "_old"))
df["delta"] = df["result_new"] - df["result_old"]


# all_deltas(df)


# print(df.columns)
# print(df["delta"].describe())

# print("Amount of nevative results: ", len(df[df["delta"] < 0.0]))
# print("Amount of neutral results: ", len(df[df["delta"] == 0]))
# print("Amount of positive results: ", len(df[df["delta"] > 0.0]))

for row in df[df["delta"] < 0.0].iterrows():
   print(row[1]["url_homepage"])
# Plot distribution of delta


# metrics for topic analysis

import pdb; pdb.set_trace()

# 2️⃣ Prepare your data
df = pd.read_json("C:\\Users\\naims\\Documents\\vdl\\vdl-project-template\\old_vs_new_summaries.json") 
print(len(df))
df = df[df["website_summary_new"].notna()]  # Filter out rows with no new summary
print(len(df))

def func (x):
    ans = ""
    try:
       ans += "*Short Summary:*\n" + x["summary"] + "\n"
       ans += "*Technologies:*\n" + ",".join( x["technologies"] ) + "\n"
       ans += "*Products:*\n" + ",".join( x["products"] ) + "\n"
       ans += "*Communities:*\n" + ",".join( x["communities"] ) + "\n"
       ans += "*Incluson and Justice:*\n" + ",".join( x["justice"] ) + "\n"

    except Exception as e:
        print(f"Error processing item: {e}")
    return ans
df["new_joined"] = df["website_summary_new"].apply(func)

results = []
print(df.columns)

for idx, item in df.iterrows():
  print(idx)
  user_prompt = f"WEBSITE TEXT:\n{item['scraped_text']}\n\nGENERATED SUMMARY\n{item['website_summary_old']}:"
  if item['website_summary_old']:
    try:
        response = client.chat.completions.create(
            model="gpt-4.1-mini",  # or any model you prefer
            messages=[ 
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.2,
        )
        content = response.choices[0].message.content.strip()

        if content:
          if content.startswith("```json"):
            content = content[7:-3].strip()
          try:
              result = json.loads(content)
              result["url_homepage"] = item["url_homepage"]
          except json.JSONDecodeError as e:
              print(f"JSON decode failed: {content}")
              result = {"error": "Invalid JSON format", "raw_output": content}
        else:
            result = {"error": "Empty response from model"}
        results.append(result)
    except Exception as e:
        print(f"Error on item {idx}: {e}")
        results.append({"error": str(e), "url_homepage": item["url_homepage"]})
  else:
    results.append({"error": "No structured summary", "url_homepage": item["url_homepage"]})
    import pdb; pdb.set_trace()

# 4️⃣ Retrieve results
# results = client.evals.runs.retrieve(run_id)
df = pd.DataFrame(results)
df.to_json("eval_for_old2.json", orient='records', indent=2)
# json.dumps(results, open("eval_results.json", "w"), indent=2)
