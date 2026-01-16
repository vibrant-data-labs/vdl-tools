from collections import defaultdict

from flashtext import KeywordProcessor
import pandas as pd
from sklearn.metrics import precision_score, recall_score


def run_keyword_extraction(
    keywords,
    df,
    text_field: str,
):
    df = df.copy()
    keyword_processor = KeywordProcessor(case_sensitive=False)
    keyword_processor.add_keywords_from_list(keywords)

    df['KW_Extracted'] = df[text_field].apply(lambda x: set(keyword_processor.extract_keywords(x)))
    df['KW_Extracted_Len'] = df['KW_Extracted'].apply(len)
    df['KW_HIT_Pos'] = df['KW_Extracted_Len'].apply(lambda x: x > 0)
    return df


def run_keyword_search_metrics(
    keywords,
    df,
    text_field: str,
    label_field: str,
):
    df = run_keyword_extraction(
        keywords,
        df,
        text_field=text_field,
    )
    precision = precision_score(
        y_true = df[label_field],
        y_pred = df['KW_HIT_Pos'],
    )
    recall = recall_score(
        y_true = df[label_field],
        y_pred = df['KW_HIT_Pos'],
    )
    counts = df.groupby(['KW_HIT_Pos', label_field]).size()
    counts.name = 'count'
    return precision, recall, counts, df

def simulate_efficiency_cuts(annotated_df, id_field):
    # 1. Calculate base stats per term
    df = annotated_df[annotated_df['KW_Extracted'].notnull()].copy()

    # Updated: Track FP indices to handle overlap correctly
    term_stats = defaultdict(lambda: {
        'TP': 0, 
        'FP': 0, 
        "TP_Doc_IDXs": set(), 
        'TP_Docs': set(),
        'FP_Doc_IDXs': set()
    })
    
    total_relevant_docs = df['RELEVANCE_LABEL'].sum()
    
    # Map terms to stats
    for idx, row in df.iterrows():
        is_relevant = row['RELEVANCE_LABEL']
        for term in row['KW_Extracted']:
            if is_relevant:
                term_stats[term]['TP'] += 1
                term_stats[term]['TP_Doc_IDXs'].add(idx)
                term_stats[term]['TP_Docs'].add(row[id_field])
            else:
                term_stats[term]['FP'] += 1
                term_stats[term]['FP_Doc_IDXs'].add(idx)

    # 2. Create the Efficiency Table
    stats_list = []
    for term, data in term_stats.items():
        tp = data['TP']
        fp = data['FP']
        cost_ratio = fp / tp if tp > 0 else float('inf')

        stats_list.append({
            'term': term,
            'TP': tp,
            'FP': fp,
            'Cost_Ratio': cost_ratio,
            'Relevant_Doc_IDXs': data['TP_Doc_IDXs'],
            'Relevant_Doc_IDs': data['TP_Docs'],
            'FP_Doc_IDXs': data['FP_Doc_IDXs'] # <--- NEW: Pass this to DF
        })

    efficiency_df = pd.DataFrame(stats_list)

    # 3. Simulation: Remove "Most Expensive" terms first
    efficiency_df.sort_values('Cost_Ratio', ascending=False, inplace=True)

    # Track Global Recall
    current_relevant_docs = set().union(*efficiency_df['Relevant_Doc_IDXs'])
    start_recall_count = len(current_relevant_docs)

    # Track Global False Positives (The Fix)
    removed_fp_indices = set()

    simulation_log = []

    print(f"Simulating removal of {len(efficiency_df)} terms...")

    # Build reverse map: DocID -> Count of Terms finding it (for Recall logic)
    doc_coverage_count = defaultdict(int)
    for idx, row in df[df['RELEVANCE_LABEL']].iterrows():
        doc_coverage_count[idx] = len(set(row['KW_Extracted']))

    terms_removed_count = 0

    for _, row in efficiency_df.iterrows():
        term = row['term']

        # 1. Update False Positive Savings (The Fix)
        # We add the indices of the bad docs found by this term to our set.
        # Since it's a set, duplicates (overlap) are automatically handled.
        removed_fp_indices.update(row['FP_Doc_IDXs'])
        actual_fp_removed_count = len(removed_fp_indices)

        # 2. Check impact on Relevant Docs (Recall)
        docs_found_by_term = row['Relevant_Doc_IDXs']
        lost_docs = 0

        for doc_id in docs_found_by_term:
            doc_coverage_count[doc_id] -= 1
            if doc_coverage_count[doc_id] == 0:
                lost_docs += 1

        start_recall_count -= lost_docs
        current_recall_pct = start_recall_count / total_relevant_docs

        terms_removed_count += 1

        # Log every 10 terms or if major drop
        if terms_removed_count % 10 == 0 or lost_docs > 0:
            simulation_log.append({
                'Terms_Removed': terms_removed_count,
                'Last_Cost_Ratio': row['Cost_Ratio'],
                'FP_Hits_Avoided': actual_fp_removed_count,
                'Recall_Pct': current_recall_pct
            })

    return pd.DataFrame(simulation_log), efficiency_df
