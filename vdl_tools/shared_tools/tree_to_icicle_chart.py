# TODO: move to scrape-enrich, next to primer api utils
from plotly import graph_objects as go

def get_basic_layout(
    title,
    height=600,
    width=950,
    font_size=11,
):
    return {
        'height': height,
        "width": width,
        "title": {"text": title, "x": .5,},
        "font": {
            "family": '"ibm-plex-sans","Nunito Sans","Roboto","Helvetica Neue",Arial,sans-serif',
            "size": font_size,
        },
        "plot_bgcolor": "white",
    }

def create_icicle_chart(tree, title="Topic Tree", show_chart=True):

    ids, labels, values = zip(*[
        (n.identifier, str(n.tag), n.data.n_occurrences)
        for n in tree.all_nodes()
    ])
    parents = [str(tree.parent(id_).identifier) if tree.parent(id_) is not None else "" for id_ in ids]
    ids = [str(i) for i in ids]

    # You can use this if you have a dictionary you want to use to re-label
    # labels = [tag_renaming_dict.get(label, label).title() for label in labels]

    icicle_fig = go.Figure(
        go.Icicle(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            tiling = dict(
                orientation='v',
                flip='x'
            ),
            root_color="#C5C9CC",
        ),
        layout=get_basic_layout(title=title, height=900, width=1200, font_size=11)
    )
    icicle_fig.update_layout(
        iciclecolorway = ["#2581D9", "#E84141"],
    )
    # write interactive chart to html 
    with open('./results/icicle.html', 'w') as f:
        f.write(icicle_fig.to_html())
    if show_chart:
        icicle_fig.show()
    return icicle_fig


def create_sunburst_chart(tree, title="Topic Tree", show_chart=True, outpath = './results/sunburst.html'):

    ids, labels, values = zip(*[
        (n.identifier, str(n.tag), n.data.n_occurrences)
        for n in tree.all_nodes()
    ])
    parents = [str(tree.parent(id_).identifier) if tree.parent(id_) is not None else "" for id_ in ids]
    ids = [str(i) for i in ids]

    # You can use this if you have a dictionary you want to use to re-label
    # labels = [tag_renaming_dict.get(label, label).title() for label in labels]

    sunburst_fig = go.Figure(
        go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            root_color="#C5C9CC",
        ),
        layout=get_basic_layout(title=title, height=900, width=1200, font_size=11)
    )
    sunburst_fig.update_layout(
        iciclecolorway = ["#2581D9", "#E84141"],
        margin = dict(t=0, l=0, r=0, b=0)
    )

    #write interactive chart to html file
    with open(outpath, 'w') as f:
        f.write(sunburst_fig.to_html())

    if show_chart:
        sunburst_fig.show()
    return sunburst_fig