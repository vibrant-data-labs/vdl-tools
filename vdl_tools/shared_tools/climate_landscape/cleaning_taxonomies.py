from vdl_tools.shared_tools.tools.logger import logger


def _join_cols(x):
    results = []
    for val in x:
        if val is not None:
            results.extend(val)
    return results


def combine_one_earth_solution_tags(ndf):
    logger.info('combining one earth tags')
    # rename and join solution and sub-pillar tags from lists
    ndf = ndf.rename(
        columns={
            'all_level0_one_earth_category': 'One Earth Pillars',
            'all_level1_one_earth_category': 'One Earth Sub-Pillars',
            'all_level2_one_earth_category': 'One Earth Solutions',
            "level0_one_earth_category": "One Earth Pillar",
            "level1_one_earth_category": "One Earth Sub-Pillar",
            "level2_one_earth_category": "One Earth Solution",
            'all_level0_FalseSolns': 'One Earth False Solutions',
        }
    )

    # ndf['One Earth Levers of Change'], ndf['One Earth Sub-Pillars'] = zip(*ndf['One Earth Sub-Pillars'].apply(split_out_levers_of_change))

    cols = ['One Earth Pillars', 'One Earth Sub-Pillars']
    ndf['One Earth Pillars & Sub-Pillars'] = ndf[cols].apply(_join_cols, axis=1)
    oe_cols = ['One Earth Pillars',
               'One Earth Sub-Pillars',
               'One Earth Solutions',
               'One Earth False Solutions',
               ]
    ndf['One Earth Tags'] = ndf[oe_cols].apply(_join_cols, axis=1)
    return ndf

def clean_one_earth_levers_of_change(ndf):
    logger.info('renaming one earth levers of change')
    # rename and join solution and sub-pillar tags from lists
    ndf = ndf.rename(
        columns={
            "level0_Levers": "One Earth Lever of Change",
            "all_level0_Levers": 'One Earth Levers of Change',
        }
    )
    return ndf


def combine_one_earth_false_solutions(ndf):
    logger.info('combining one earth false solutions')
    # rename and join solution and sub-pillar tags from lists
    ndf = ndf.rename(
        columns={
            'all_level0_FalseSolns': 'One Earth False Solutions',
        }
    )

    return ndf

def split_theme_for_leaf_node(theme_list):
    cleaned_themes = []
    for theme in theme_list:
        if ':' in theme:
            theme = theme.split(':')[1]
        else:
            theme = theme
        cleaned_themes.append(theme)
    return cleaned_themes


def combine_one_earth_intersectional_themes(ndf):
    logger.info('combining one earth intersectional themes')
    # rename and join solution and sub-pillar tags from lists
    ndf = ndf.rename(
        columns={
            'all_level0_Intersectional': 'One Earth Intersectional Themes',
            'all_level1_Intersectional': 'One Earth Intersectional Sub-Themes',
            "level0_Intersectional": "One Earth Intersectional Theme",
            "level1_Intersectional": "One Earth Intersectional Sub-Theme",
        }
    )

    cols = ['One Earth Intersectional Themes', 'One Earth Intersectional Sub-Themes']
    ndf['One Earth Intersectional Themes Areas'] = ndf[cols].apply(_join_cols, axis=1)
    ndf['One Earth Intersectional Themes Areas'] = ndf['One Earth Intersectional Themes Areas'].apply(
        split_theme_for_leaf_node
    )
    return ndf


# def clean_pillars_and_subpillar_categories(df):
#     df['level0_one_earth_category'] = df['level0_one_earth_category'].str.replace("Cross-Cutting (Root Node)",
#                                                                                     "Cross-Cutting")
#     df['level0_one_earth_category'] = df['level0_one_earth_category'].str.replace("No_Level_0", "No One Earth Match")
#     df['level1_one_earth_category'] = df['level1_one_earth_category'].apply(
#         lambda x: None if "No_Level_1_" in x else x)
#     df['level1_one_earth_category'] = df['level1_one_earth_category'].str.replace('Industries & Services: ', '',
#                                                                                     regex=False)
#     df['level1_one_earth_category'] = df['level1_one_earth_category'].str.replace('Industries & Services',
#                                                                                     'Industries & Services Cross-Cutting',
#                                                                                     regex=False)
#     df['level1_one_earth_category'] = df['level1_one_earth_category'].str.replace('Cross-Cutting Nature',
#                                                                                     'Nature Cross-Cutting', regex=False)
#     df['level1_one_earth_category'] = df['level1_one_earth_category'].str.replace('Cross-Cutting Regen. Ag.',
#                                                                                     'Regen. Ag. Cross-Cutting',
#                                                                                     regex=False)
#     df['level1_one_earth_category'] = df['level1_one_earth_category'].str.replace('Cross-Cutting Energy',
#                                                                                     'Energy Cross-Cutting', regex=False)
#     return df


def rename_one_earth_crosscutting_tags(df):
    logger.info('renaming one earth crosscutting tags')
    # single best match (category)
    tax_cat_cols = [
        'level0_one_earth_category',
        'level1_one_earth_category',
        ]
    # multiple matches (list of tags)
    tax_tag_cols = [
        'all_level0_one_earth_category',
        'all_level1_one_earth_category',
        'all_level2_one_earth_category',
                ]
    rename_dict = {
        " (Root Node)": '',
        "No_Level_0": "No One Earth Match",
        'Cross-Cutting Nature': 'Nature Cross-Cutting',
        'Cross-Cutting Regen. Ag.': 'Regen. Ag. Cross-Cutting',
        'Cross-Cutting Energy': 'Energy Cross-Cutting',
        'Industries & Services: ': '',
        'Industries & Services': 'Industries & Services Cross-Cutting',
    }
    for col in tax_cat_cols:
        # rename values
        for k, v in rename_dict.items():
            df[col] = df[col].str.replace(k, v, regex=False)
    for col in tax_tag_cols:
        # rename values
        for k, v in rename_dict.items():
            # fill empty cells with empty list
            df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])
            # strip of empty strings
            df[col] = df[col].apply(lambda x: [tag for tag in x if tag != ''])
            # replace the key with the value
            df[col] = df[col].apply(lambda x: [tag.replace(k, v) for tag in x] if isinstance(x, list) else x)
    return df


def clean_no_level(df,
                   taxonomy="one_earth",
                   level=1  # e.g., 1 for No_Level_1
                   ):
    level = str(level)
    logger.info(f'cleaning no level {level} tags')
    # clean no level 1 categories
    df[f'level{level}_{taxonomy}_category'] = df[f'level{level}_{taxonomy}_category'].apply(
        lambda x: None if f"No_Level_{level}_" in x else x)
    # clean no level 0 tags
    df[f'all_level{level}_{taxonomy}_category'] = df[f'all_level{level}_{taxonomy}_category'].apply(
        lambda x: [tag for tag in x if "No_Level_{level}_" not in tag] if isinstance(x, list) else x)

    return df