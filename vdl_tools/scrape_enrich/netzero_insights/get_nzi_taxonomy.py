from typing import List, Dict

import pandas as pd

from vdl_tools.scrape_enrich.netzero_insights.search_netzero_api import get_netzero_api


def get_taxonomy_flat(
    tree: List[Dict], 
    parent_id: int,
    parent_name: str,
    limit: int = 10
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
    return flatten_nodes(tree, parent_id, parent_name)


if __name__ == "__main__":
    api_client = get_netzero_api()
    OUTPUT_PATH = '../shared-data/data/taxonomies/netzero/full_taxonomy.xlsx'
    full_tree = api_client.get_taxonomy_children_recursive(660, limit=None)
    flat_taxonomy = get_taxonomy_flat(full_tree, 660, 'Vertical')
    taxonomy_df = pd.DataFrame(flat_taxonomy)

    with pd.ExcelWriter(OUTPUT_PATH) as writer:
        for depth in range(taxonomy_df['depth'].max() +1):
            depth_df = taxonomy_df[taxonomy_df['depth'] == depth][['id', 'label', 'parent_id', 'parent_name', 'description', 'depth']].copy()
            depth_df.rename(columns={"label": f'level_{depth}', "parent_name": f'level_{depth-1}'}, inplace=True)
            if depth == 0:
                depth_df.pop(f'level_{depth-1}')
            depth_df.to_excel(writer, sheet_name=f'level_{depth}', index=False)