from dataclasses import dataclass


@dataclass
class ClusteringParams:
    method: str = 'louvain'
    resolution: float = 1.0        # leiden resolution parameter, if list then compute multiple levels
    merge_tiny: bool = False
    name_prefix: str = 'Cluster'
    reassign_size_ratio: int = 10  # size ratio between big and small (if small is less than 10% of big, reassign)
    reassign_top_n: int = 5        # top n most similar clusters to check for reassignment
    reassign_max_size: int = 40    # if cluster is bigger than that don't re-assign it (even if size ratio is met)
    min_clus_size: int = 100        # hierarchical leiden min cluster size to split; if list one value per level
