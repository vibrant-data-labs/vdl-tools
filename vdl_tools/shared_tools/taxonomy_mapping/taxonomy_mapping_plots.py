import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def level_0_plot(df, level_colors, display_map, funding_attr, l0_attr='level0', l0_name='Pillar', level_order=[2, 0, 1]):
    # pillar plot data prep
    df['color'] = df[l0_attr].map(level_colors)
    df['displaypillar'] = df[l0_attr].map(display_map)
    if level_order is not None:
        df = df.iloc[level_order]
    df[funding_attr] /= 1e9

    # pillar plot
    fig, ax = plt.subplots(figsize=(5, 6))
    ax.bar(df.displaypillar, df[funding_attr], 0.5, align='center', color=df.color)
    # ax.legend(loc='upper left', bbox_to_anchor=(0, 1))
    ax.set_title(f"Funding by {l0_name}")
    ax.set_ylabel("Funding ($ Billions)", fontsize=16)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(axis='x', length=0)

    # set x tick label font colors
    tick_labels = ax.get_xticklabels()
    tick_colors = df['color']
    for xtick, color in zip(tick_labels, tick_colors):
        xtick.set_color(color)
    
    return fig


# %%


def level_1_plot(df, l0_colors, l0_idx, l0_map, l1_map, funding_attr, l1_name='Subpillar', rotate=False, prefix=""):
    rotation = 90 if rotate else 0
    pctfunding = "pctfunding"
    # get the subpillar data ready for plotting
    df[pctfunding] = 100 * df[funding_attr] / df[funding_attr].sum()
    df['color'] = df.level0.map(l0_colors)
    df['displayl0'] = df.level0.map(l0_map)
    df['displayl1'] = df.level1.map(l1_map)
    df['l0_idx'] = df.level0.map(l0_idx)
    df.sort_values(['l0_idx', 'level1'], inplace=True)

    p_counts = df.groupby('level0')['level1'].count()[df.level0.unique()]
    p_fracs = p_counts.cumsum().values
    p_fracs = np.concatenate([np.array([0]), p_fracs / p_fracs[-1]])

    # level1 (subpillar) plot
    fig, ax = plt.subplots(figsize=(12, 6))
    df.plot.bar('level1', pctfunding, width=0.7,
                color=df.color, ax=ax, legend=None)

    ax.set_xticklabels(df.displayl1, rotation=90, fontsize=8)
    ax.set_ylabel("% of Total Funding", fontsize=16)
    ax.set_xlabel("")

    ax.set_title(f"{prefix} Funding % by {l1_name}")
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(axis='x', length=0)

    # duplicate x-axis for level0 (pillar) labels
    if not rotate:
        ax2 = ax.twiny()

        ax2.spines["bottom"].set_position(("axes", -0.3 if rotate else -0.15))
        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        ax2.tick_params('both', length=0, width=0, which='minor')
        ax2.tick_params('both', direction='in', which='major')
        ax2.xaxis.set_ticks_position("bottom")
        ax2.xaxis.set_label_position("bottom")

        ax2.set_xticks(p_fracs)
        ax2.xaxis.set_major_formatter(ticker.NullFormatter())
        ax2.xaxis.set_minor_locator(ticker.FixedLocator((p_fracs[0:-1] + p_fracs[1:]) / 2))
        ax2.xaxis.set_minor_formatter(ticker.FixedFormatter(df.displayl0.unique()))

        for xtick, color in zip(ax2.get_xticklabels('minor'), df['color'].unique()):
            xtick.set_color(color)
            xtick.set_fontsize(14)
        
    # Adjust plot margins
    plt.subplots_adjust(bottom=0.25)
    return fig


# %%


def level_2_plot(df, l0_colors, l0_idx, l0_map, l1_map, funding_attr, l2_name='Solution'):
    pctfunding = 'pctfunding'
    # solution plot data prep
    df[pctfunding] = 100 * df[funding_attr] / df[funding_attr].sum()
    df['color'] = df.level0.map(l0_colors)
    df['display_l0'] = df.level0.map(l0_map)
    df['display_l1'] = df.level1.map(l1_map)
    df['l0_idx'] = df.level0.map(l0_idx)
    df.sort_values(['level0', 'level1', 'level2'], inplace=True)

    sp_counts = df.groupby('level1')['level2'].count()[df.level1.unique()]
    sp_fracs = sp_counts.cumsum().values
    sp_fracs = np.concatenate([np.array([0]), sp_fracs / sp_fracs[-1]])

    p_counts = df.groupby('level0')['level2'].count()[df.level0.unique()]
    p_fracs = p_counts.cumsum().values
    p_fracs = np.concatenate([np.array([0]), p_fracs / p_fracs[-1]])

    # level2 (solution) plot
    fig, ax = plt.subplots(figsize=(14, 6))
    df.plot.bar('level2', pctfunding, color=df.color, width=0.9, ax=ax, legend=None)

    ax.set_xticklabels(df.level2, fontsize=6)
    ax.set_ylabel("% of Total Funding", fontsize=16)
    ax.set_xlabel("")

    ax.set_title(f"Funding % by {l2_name}")
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(axis='x', length=0)

    # duplicate x-axis for level1 (subpillar) labels
    ax2 = ax.twiny()

    ax2.spines["bottom"].set_position(("axes", -0.30))
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.tick_params('both', length=0, width=0, which='minor')
    ax2.tick_params('both', direction='in', which='major')
    ax2.xaxis.set_ticks_position("bottom")
    ax2.xaxis.set_label_position("bottom")

    ax2.set_xticks(sp_fracs)
    ax2.xaxis.set_major_formatter(ticker.NullFormatter())
    ax2.xaxis.set_minor_locator(ticker.FixedLocator((sp_fracs[0:-1] + sp_fracs[1:]) / 2))
    ax2.xaxis.set_minor_formatter(ticker.FixedFormatter(df.display_l1.unique()))

    for xtick in ax2.get_xticklabels('minor'):
        xtick.set_fontsize(8)

    # duplicate x-axis for pillar labels
    if df.level0.nunique() > 1:
        ax3 = ax.twiny()

        ax3.spines["bottom"].set_position(("axes", -0.40))
        ax3.spines['top'].set_visible(False)
        ax3.spines['right'].set_visible(False)
        ax3.tick_params('both', length=0, width=0, which='minor')
        ax3.tick_params('both', direction='in', which='major')
        ax3.xaxis.set_ticks_position("bottom")
        ax3.xaxis.set_label_position("bottom")

        ax3.set_xticks(p_fracs)
        ax3.xaxis.set_major_formatter(ticker.NullFormatter())
        ax3.xaxis.set_minor_locator(ticker.FixedLocator((p_fracs[0:-1] + p_fracs[1:]) / 2))
        ax3.xaxis.set_minor_formatter(ticker.FixedFormatter(df.display_l0.unique()))

        for xtick, color in zip(ax3.get_xticklabels('minor'), l0_colors.values()):
            xtick.set_color(color)
            xtick.set_fontsize(14)
    #plt.subplots_adjust(bottom=0.45)
    return fig
