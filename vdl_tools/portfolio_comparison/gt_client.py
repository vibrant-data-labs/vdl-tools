"""GT datamart client construction — one place, two credential sources.

Order: ``GT_DATAMART_PG_*`` env vars (the client's native mechanism), then a
``[gt_datamart]`` section in the VDL config.ini (host/port/user/password/
database). Every portfolio_comparison lane that talks to the datamart builds
its client here, so engagement commands run bare — no env injection ritual.
"""


def make_gt_client():
    from givingtuesday_datamart.client.client import GtDatamartClient

    try:
        return GtDatamartClient()
    except RuntimeError:
        pass  # env vars not set — fall through to config.ini

    from vdl_tools.shared_tools.tools.config_utils import get_configuration

    cfg = get_configuration()
    if "gt_datamart" not in cfg:
        raise RuntimeError(
            "GT datamart credentials not found. Either export GT_DATAMART_PG_"
            "HOST/USER/PASSWORD (and optionally PORT/DATABASE), or add a "
            "[gt_datamart] section with host/port/user/password/database to "
            "your VDL config.ini."
        )
    section = cfg["gt_datamart"]
    return GtDatamartClient(
        host=section.get("host"),
        port=int(section.get("port", 5432)),
        user=section.get("user"),
        password=section.get("password"),
        database=section.get("database", "gt_datamart"),
    )
