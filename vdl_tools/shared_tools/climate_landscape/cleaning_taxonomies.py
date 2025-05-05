from vdl_tools.shared_tools.tools.logger import logger


def _join_cols(x):
    results = []
    for val in x:
        if val is not None:
            results.extend(val)
    return results


#def split_out_levers_of_change(subpillars_list):
#    levers_of_change = []
#    clean_subpillars = []
#    if not subpillars_list:
#        return None, None
#    for subpillar in subpillars_list:
#        # There is one named Root Node Science and Technology
#        if subpillar.startswith("Root Node "):
#            levers_of_change.append(subpillar.strip("Root Node ").strip())
#        elif subpillar.startswith("Root "):
#            levers_of_change.append(subpillar.strip("Root ").strip())
#
#        # Don't need to include Cross-Cutting Subpillars
#        elif subpillar.startswith("Cross-Cutting "):
#            continue
#        else:
#            clean_subpillars.append(subpillar)
#    if not levers_of_change:
#        levers_of_change = None
#    if not clean_subpillars:
#        clean_subpillars = None
#    return levers_of_change, clean_subpillars


def combine_one_earth_tags(ndf):
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
            "all_level0_Levers": 'One Earth Levers of Change'
        }
    )

    # ndf['One Earth Levers of Change'], ndf['One Earth Sub-Pillars'] = zip(*ndf['One Earth Sub-Pillars'].apply(split_out_levers_of_change))

    cols = ['One Earth Pillars', 'One Earth Sub-Pillars']
    ndf['One Earth Pillars & Sub-Pillars'] = ndf[cols].apply(_join_cols, axis=1)
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


