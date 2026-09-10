"""Deprecated: use vdl_tools.causal_networks.metrics.node_pair_similarities. Will be removed in 3.0."""
import warnings
from vdl_tools.causal_networks.metrics import node_pair_similarities  # noqa: F401

warnings.warn("vdl_tools.network_tools.node_similarity is deprecated; use vdl_tools.causal_networks.metrics",
              DeprecationWarning, stacklevel=2)


def jaccardianSim(nw, identicalThresh=0.3, deleteIdentical=False):
    """Deprecated alias for node_pair_similarities; returns a list of (id_1, id_2, similarity) tuples."""
    sims = node_pair_similarities(nw, {n: n for n in nw.nodes()})
    return list(zip(sims["id_1"], sims["id_2"], sims["jaccard_similarity"]))
