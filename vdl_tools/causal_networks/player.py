"""
Build an openmappr network player for a scored causal network (via py2mappr).

Three snapshots: the network grouped by theme, keystone score vs upstream score, and
keystone rank vs upstream rank. Node color = the grouping column, node size = Catalytic Score.

How each attribute is shown is controlled by a settings TABLE with one row per attribute, the same
format VDL uses in other projects (a `player_attribute_settings.xlsx` with 0/1 columns):

    Attribute | Display_Name | Keep | visible_filters | visible_profile | visible_search | free_text |
    tag_list | tags_4 | tags_3 | tags_2 | tags_1 | horizontal_bars | years | urls | axis_select |
    color_select | size_select | email | tooltip

`DEFAULT_ATTRIBUTE_SETTINGS` below is that table for the columns the pipeline produces. Write it out
with `write_attribute_settings(path)`, edit it in Excel, and pass the path back to `build_player`.
"""

import http.server
import socketserver
import webbrowser
from pathlib import Path

import pandas as pd

import vdl_tools.py2mappr as mappr
import vdl_tools.py2mappr.publish as publish
import vdl_tools.py2mappr.vdl_palette as pal


# 0/1 setting columns in the table -> keyword argument of py2mappr's project.update_attributes
SETTING_COLUMNS = {
    "visible_filters": "visible_filters",   # show in the left filter panel
    "visible_profile": "visible_profile",   # show in the node profile (right panel)
    "visible_search": "visible_search",     # searchable
    "free_text": "text_str",                # render as free text
    "tag_list": "list_string",              # parse as a list of tags
    "tags_4": "tag_cloud",                  # tag cloud, 4 per row
    "tags_3": "tags_3",                     # tag cloud, 3 per row
    "tags_2": "tags_2",                     # tag cloud, 2 per row
    "tags_1": "wide_tags",                  # one wide tag per row
    "horizontal_bars": "horizontal_bars",   # horizontal bar chart (categorical)
    "years": "years",                       # render as a year
    "urls": "urls",                         # clickable link
    "axis_select": "axis_select",           # offered as a scatterplot axis
    "color_select": "color_select",         # offered in the color-by dropdown
    "size_select": "size_select",           # offered in the size-by dropdown
    "email": "email",                       # clickable email
}

# one row per attribute; 1 = on. Columns not listed for a row are 0.
_DEFAULT_ROWS = [
    # Attribute,                 settings...                                                      tooltip
    dict(Attribute="Root Factor", visible_profile=1, visible_search=1, free_text=1,
         tooltip="The factor as worded in the survey."),
    dict(Attribute="Name", visible_search=1, free_text=1, tooltip="Short display name."),
    dict(Attribute="Pillar", visible_filters=1, visible_profile=1, visible_search=1, horizontal_bars=1, color_select=1,
         tooltip="Theme the factor was grouped into."),
    dict(Attribute="Catalytic Score", visible_filters=1, visible_profile=1, axis_select=1, color_select=1, size_select=1,
         tooltip="Keystone Rank x Upstream Score: high leverage AND upstream (0-100)."),
    dict(Attribute="Top Keystone", visible_filters=1, visible_profile=1, color_select=1,
         tooltip="Yes if Keystone Rank is in the top 20%."),
    dict(Attribute="Keystone Rank", visible_filters=1, visible_profile=1, axis_select=1, color_select=1, size_select=1,
         tooltip="Percentile of Keystone Score, averaged across trials (0-100)."),
    dict(Attribute="Keystone Score", visible_profile=1, axis_select=1, size_select=1,
         tooltip="Reach in 2 hops x 2-hop asymmetry (few incoming, many outgoing), scaled 0-1."),
    dict(Attribute="Upstream Rank", visible_filters=1, visible_profile=1, axis_select=1, color_select=1,
         tooltip="Percentile of Upstream Score, averaged across trials (0-100). High = more upstream."),
    dict(Attribute="Upstream Score", visible_profile=1, axis_select=1,
         tooltip="1 - Trophic Level: 1 = root cause, 0 = downstream symptom."),
    dict(Attribute="Trophic Level", visible_profile=1, axis_select=1,
         tooltip="Causal flow position: 0 = most upstream, 1 = most downstream."),
    dict(Attribute="Degree", visible_filters=1, visible_profile=1, axis_select=1, size_select=1,
         tooltip="Number of links in the ensemble network."),
    dict(Attribute="Incoming Links", visible_profile=1, axis_select=1, tooltip="Factors that influence this one."),
    dict(Attribute="Outgoing Links", visible_profile=1, axis_select=1, tooltip="Factors this one influences."),
    dict(Attribute="Reach in 2 Hops (Pct)", visible_filters=1, visible_profile=1, axis_select=1, size_select=1,
         tooltip="Percent of all factors reachable within 2 outgoing links."),
    dict(Attribute="Betweenness", axis_select=1, tooltip="Betweenness centrality in the ensemble network."),
    dict(Attribute="Causal Cluster", visible_filters=1, visible_profile=1, horizontal_bars=1, color_select=1,
         tooltip="Community of factors more densely linked to each other than to the rest."),
    dict(Attribute="Cluster Bridging"),
    dict(Attribute="Cluster Centrality"),
    dict(Attribute="Keystone Rank (Std Dev)", tooltip="Spread of Keystone Rank across monte-carlo trials."),
    dict(Attribute="Upstream Rank (Std Dev)", tooltip="Spread of Upstream Rank across monte-carlo trials."),
    dict(Attribute="n_trials"),
    dict(Attribute="uuid"),
    dict(Attribute="label"),
    dict(Attribute="x"),
    dict(Attribute="y"),
    dict(Attribute="id"),
]
_TABLE_COLUMNS = ["Attribute", "Display_Name", "Keep"] + list(SETTING_COLUMNS) + ["tooltip"]

DEFAULT_ATTRIBUTE_SETTINGS = pd.DataFrame(_DEFAULT_ROWS).reindex(columns=_TABLE_COLUMNS)
DEFAULT_ATTRIBUTE_SETTINGS["Display_Name"] = DEFAULT_ATTRIBUTE_SETTINGS["Attribute"]
DEFAULT_ATTRIBUTE_SETTINGS["Keep"] = 1
DEFAULT_ATTRIBUTE_SETTINGS[list(SETTING_COLUMNS)] = DEFAULT_ATTRIBUTE_SETTINGS[list(SETTING_COLUMNS)].fillna(0).astype(int)


def write_attribute_settings(path, settings=None):
    """Write the attribute settings table (default: DEFAULT_ATTRIBUTE_SETTINGS) to .xlsx or .csv, to edit and reuse."""
    table = DEFAULT_ATTRIBUTE_SETTINGS if settings is None else settings
    path = Path(path)
    if path.suffix.lower() == ".csv":
        table.to_csv(path, index=False)
    else:
        table.to_excel(path, index=False)
    print(f"Wrote attribute settings to {path}")


def read_attribute_settings(settings):
    """
    Turn a settings table (DataFrame, .xlsx or .csv path) into what build_player needs:
      rename   {Attribute: Display_Name} for rows with Keep == 1 (only where the two differ)
      drop     attributes with Keep == 0
      kwargs   for project.update_attributes (lists of display names per setting, plus attr_descriptions)
    Missing setting columns count as 0; a missing Keep column keeps every row.
    """
    if isinstance(settings, (str, Path)):
        path = Path(settings)
        table = pd.read_csv(path) if path.suffix.lower() == ".csv" else pd.read_excel(path)
    else:
        table = settings.copy()
    table = table.reindex(columns=_TABLE_COLUMNS)  # add any missing columns as NaN
    table["Display_Name"] = table["Display_Name"].fillna(table["Attribute"])
    table["Keep"] = table["Keep"].fillna(1).astype(int)
    table[list(SETTING_COLUMNS)] = table[list(SETTING_COLUMNS)].fillna(0).astype(int)

    kept = table[table["Keep"] == 1]
    rename = {a: d for a, d in zip(kept["Attribute"], kept["Display_Name"]) if a != d}
    drop = table.loc[table["Keep"] == 0, "Attribute"].tolist()
    kwargs = {arg: kept.loc[kept[col] == 1, "Display_Name"].tolist() for col, arg in SETTING_COLUMNS.items()}
    with_tooltip = kept[kept["tooltip"].notna()]
    kwargs["attr_descriptions"] = dict(zip(with_tooltip["Display_Name"], with_tooltip["tooltip"]))
    return rename, drop, kwargs


def build_player(nodes, links, out_dir,
                 title,                       # project title shown in the player
                 description="",              # project description (html allowed)
                 group_attr="Pillar",         # categorical node column for colors and the grouped snapshot
                 size_attr="Catalytic Score", # numeric node column for node size
                 logo_image_url=None, logo_url=None, feedback_email=None,
                 attribute_settings=None,     # settings table (DataFrame, .xlsx or .csv); default: DEFAULT_ATTRIBUTE_SETTINGS
                 node_size_scaling=(5, 15, 1),
                 launch_local=False,          # serve the player in the browser right after building (blocks until Ctrl-C)
                 s3_bucket=None,              # upload to this S3 bucket after building (needs [aws] config)
                 ):
    """
    Build an openmappr player into `out_dir` from the display tables returned by pipeline.py.
    `nodes` must have `id`, `label`, `x`, `y`; `links` must have `Source`, `Target`.
    Attributes absent from the settings table are kept but shown nowhere (no filter, profile or search).

    `launch_local=True` serves the player and blocks, so nothing after this call runs until you stop it.
    When a script builds several players, build them all with launch_local=False and call
    `serve_player(out_dir)` once at the end instead.
    """
    # apply the settings table: rename / drop attributes, then keep only settings for attributes present
    rename, drop, settings = read_attribute_settings(
        DEFAULT_ATTRIBUTE_SETTINGS if attribute_settings is None else attribute_settings)
    nodes = nodes.drop(columns=[c for c in drop if c in nodes.columns]).rename(columns=rename)
    group_attr = rename.get(group_attr, group_attr)
    size_attr = rename.get(size_attr, size_attr)
    if group_attr not in nodes.columns:
        group_attr = "Causal Cluster"  # every scored network has clusters even without curated themes
    if size_attr not in nodes.columns:
        size_attr = "Keystone Rank"
    present = set(nodes.columns)
    settings = {k: ([a for a in v if a in present] if isinstance(v, list) else {a: t for a, t in v.items() if a in present})
                for k, v in settings.items()}
    for key in ("visible_filters", "visible_profile", "visible_search", "horizontal_bars", "color_select"):
        if group_attr not in settings[key]:
            settings[key].insert(0, group_attr)

    project, snap_groups = mappr.create_map(nodes, network_df=links)

    link_style = dict(link_curve=0.6, link_weight=0.4, neighbors=1, direction="all")

    # snapshot 1: the network, grouped and colored by theme
    snap_groups.set_nodes(node_color=group_attr, node_size=size_attr, node_size_scaling=node_size_scaling)
    snap_groups.set_palette(pal.cat_palette, pal.num_palette)
    snap_groups.set_links(**link_style)
    snap_groups.set_clusters(cluster_attr=group_attr)
    snap_groups.settings.update({"drawGroupLabels": True, "drawLabels": True})
    snap_groups.set_display_data(
        title=f"Grouped by {group_attr}",
        subtitle=f"Factors grouped by {group_attr}; size = {size_attr}",
        description="<p>Each node is a factor; each link means 'if the source improves, the target improves too'. "
                    f"Node size is {size_attr}.</p>")

    # snapshots 2 and 3: keystone leverage vs upstream position, as scores and as ranks
    scatter_specs = [
        ("Keystone vs Upstream (scores)", "Upstream Score", "Keystone Score"),
        ("Keystone vs Upstream (ranks)", "Upstream Rank", "Keystone Rank"),
    ]
    scatters = []
    for snap_title, x_attr, y_attr in scatter_specs:
        x_attr, y_attr = rename.get(x_attr, x_attr), rename.get(y_attr, y_attr)
        if x_attr not in present or y_attr not in present:
            continue
        snap = mappr.create_layout(layout_type="scatterplot")
        snap.x_axis = x_attr
        snap.y_axis = y_attr
        snap.set_nodes(node_color=group_attr, node_size=size_attr, node_size_scaling=(5, 20, 1))
        snap.set_palette(pal.cat_palette, pal.num_palette)
        snap.set_links(**link_style)
        snap.settings.update({"nodeImageShow": False, "nodeSizeScaleStrategy": "linear",
                              "scatterAspect": 0.6, "drawLabels": True})
        snap.set_display_data(
            title=snap_title,
            subtitle=f"{y_attr} (leverage) vs {x_attr} (causal position)",
            description="<p>The keystone factors sit in the upper right: "
                        "high keystone leverage and upstream in the causal flow.</p>")
        scatters.append(snap)
    project.snapshots = [snap_groups] + scatters

    # project-level settings
    project.set_display_data(title=title, description=description,
                             logo_image_url=logo_image_url, logo_url=logo_url)
    project.configuration.update({"showStartInfo": False, "startPage": "legend", "displayTooltipCard": False})
    project.set_export_button(True)
    if feedback_email:
        project.set_feedback({"type": "email", "link": feedback_email, "text": "contact"})
    project.update_attributes(**settings)

    # build, then optionally publish
    workers = []
    if s3_bucket:
        workers.append(publish.s3(s3_bucket))
    if launch_local:
        workers.append(publish.local())
    if workers:
        publish.run(workers, out_dir)
    else:
        mappr.build(out_folder=out_dir)
    print(f"Player built in {out_dir}")



def serve_player(out_dir, port=8000, open_browser=True):
    """
    Serve a built player folder at http://localhost:<port> and open it in the browser.
    Blocks until Ctrl-C, so call it as the last step of a run script. The page loads the openmappr
    code from mappr-player.openmappr.org, so it needs an internet connection even when served locally.
    """
    out_dir = Path(out_dir)
    if not (out_dir / "index.html").exists():
        raise FileNotFoundError(f"No player found in {out_dir}; build one with build_player() first.")
    handler = lambda *args, **kwargs: http.server.SimpleHTTPRequestHandler(*args, directory=str(out_dir), **kwargs)
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", port), handler) as httpd:
        url = f"http://localhost:{port}"
        print(f"\nServing the player at {url}  (Ctrl-C to stop)")
        if open_browser:
            webbrowser.open_new_tab(url)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopped serving the player.")
