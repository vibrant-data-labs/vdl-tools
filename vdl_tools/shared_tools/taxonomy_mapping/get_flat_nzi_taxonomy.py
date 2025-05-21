from pathlib import Path
from typing import List, Dict
import pandas as pd
from vdl_tools.scrape_enrich.netzero_insights.search_netzero_api import get_netzero_api


api_client = get_netzero_api(use_sandbox=False)

ROOT_ID = 660
ROOT_NAME = 'Vertical'


def get_taxonomy_children(parent_id: int):
    """Get taxonomy for a specific parent ID."""
    payload = {
        'onlyVisible': True,
        'onlyAdvancedFilters': False,
        'mainFilter': {
            'include': {},
            'exclude': {},
            'fundingRoundInclude': {},
            'fundingRoundExclude': {},
            'investorInclude': {},
            'investorExclude': {},
        },
        'onlySearchable': True
    }
    print(f"Getting children for {parent_id}")
    return api_client._post(
        endpoint=f"taxonomy/graph/{parent_id}",
        payload=payload
    )

def get_taxonomy_children_recursive(parent_id: int, limit: int = 10, current_depth: int = 0) :
    """Get taxonomy for a specific parent ID and all its children."""
    children = get_taxonomy_children(parent_id)
    current_depth += 1
    for child in children:
        if current_depth >= limit:
            break
        child['children'] = get_taxonomy_children_recursive(child['id'], limit, current_depth)
    return children


def get_taxonomy_flat(
    tree: List[Dict], 
    parent_id: int,
    parent_name: str,
    limit: int = 10,
    output_file: str = None
) -> List[Dict]:
    """Get a flat list of all taxonomy nodes starting from a parent ID.

    Args:
        parent_id: The ID of the parent node to start from
        limit: Maximum depth to traverse (default: 10)

    Returns:
        List of dictionaries containing all taxonomy nodes, with parent_id and depth information
    """
    def flatten_nodes(nodes: List[Dict], parent_id: int, parent_name: str, depth: int = 0) -> List[Dict]:
        flat_list = []
        for node in nodes:
            # Create a copy of the node without children
            node_copy = {k: v for k, v in node.items() if k != 'children'}
            node_copy['parent_id'] = parent_id
            node_copy['parent_name'] = parent_name
            node_copy['depth'] = depth
            flat_list.append(node_copy)

            # Recursively add children if they exist and we haven't hit the depth limit
            if 'children' in node and depth < limit:
                flat_list.extend(flatten_nodes(
                    node['children'], 
                    node['id'], 
                    node['label'], 
                    depth + 1
                ))
        return flat_list

    # Get the full tree structure
    # Flatten it into a single list
    flat_nodes = flatten_nodes(tree, parent_id, parent_name)
    df = pd.DataFrame(flat_nodes)
    df = df.drop_duplicates(subset=['id', 'parent_id'])
    if output_file:
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False)
    return df


def write_taxonomy_to_excel(taxonomy_df: pd.DataFrame, filename: str):
    """Go through each depth, and write the depth as a new sheet."""
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    out_columns = ['id', 'label', 'parent_id', 'parent_name', 'description', 'depth']
    with pd.ExcelWriter(filename) as writer:
        for depth in range(taxonomy_df['depth'].max() +1):
            depth_df = taxonomy_df[taxonomy_df['depth'] == depth][out_columns].copy()
            depth_df.rename(columns={"label": f'level_{depth}', "parent_name": f'level_{depth-1}'}, inplace=True)
            if depth == 0:
                depth_df.pop(f'level_{depth-1}')
            depth_df.to_excel(writer, sheet_name=f'level_{depth}', index=False)


if __name__ == "__main__":
    taxonomy = get_taxonomy_children_recursive(ROOT_ID)
    taxonomy_df = get_taxonomy_flat(taxonomy, ROOT_ID, ROOT_NAME, output_file='../shared-data/data/taxonomies/netzero/full_taxonomy_flat.csv')
    write_taxonomy_to_excel(taxonomy_df, '../shared-data/data/taxonomies/netzero/full_taxonomy.xlsx')

