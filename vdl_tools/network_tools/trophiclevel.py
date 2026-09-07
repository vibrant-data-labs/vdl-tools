"""Deprecated: use vdl_tools.causal_networks.metrics (rooted_trophic_level). Will be removed in 3.0."""
import warnings
from vdl_tools.causal_networks.metrics import rooted_trophic_level as rootedTL  # noqa: F401
from vdl_tools.causal_networks.metrics import min_max  # noqa: F401

warnings.warn("vdl_tools.network_tools.trophiclevel is deprecated; use vdl_tools.causal_networks.metrics",
              DeprecationWarning, stacklevel=2)


def computeTL(network):
    """Deprecated alias: unrooted trophic level is no longer provided; returns the rooted version."""
    return rootedTL(network)
