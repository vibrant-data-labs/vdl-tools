import numpy as np
import pandas as pd

import json
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.embed_texts_with_cache import embed_texts_with_cache
from vdl_tools.shared_tools.taxonomy_mapping.few_shot_cache import FewShotCache
from vdl_tools.shared_tools.tools.logger import logger


# get or compute embeddings on cft data
def get_or_compute_embeddings(
    org_df,
    id_col,
    text_col,
    used_cached_result=True,
    max_workers=3,
    embedding_provider="openai",
    embedding_model="text-embedding-3-large"
):
    ids_text = org_df[[id_col, text_col]].values.tolist()
    embeddings = embed_texts_with_cache(
        ids_texts=ids_text,
        use_cached_result=used_cached_result,
        return_flat=True,
        max_workers=max_workers,
        embedding_provider=embedding_provider,
        embedding_model=embedding_model,
    )
    return embeddings


def _get_parent(item, level, taxonomy):
    tx = taxonomy[level]
    parent = taxonomy[level-1]
    # check if item is in tx[data]
    if item not in tx['data'][tx['name']].values:
        return None
    retval = tx['data'].loc[item][parent['name']]
    return retval

def get_lineage(item, level, taxonomy, full_lineage=[]):
    """
    Recursively get the parent of the item at the given level
    until you reach the root of the taxonomy
    """
    full_lineage = full_lineage or []
    if level > 0:
        parent = _get_parent(item, level, taxonomy)
        if parent:
            full_lineage.append((parent, level - 1))
            item = parent
            level -= 1
            return get_lineage(item, level, taxonomy, full_lineage)
    return full_lineage


def _get_similarity_percentiles(
    sims,
    cats,
    id_attr,
    uids,
    nmax,
    thr,
    pct_delta,
    taxonomy,
    force_parents=False
):
    sims_distr = sims.flatten()
    sims_distr.sort()

    # max_sims = sims.max(axis=1)
    # get each row indices, descending sort
    sorted_indices = np.flip(sims.argsort(axis=1), axis=1)
    pcts = 100 * np.searchsorted(sims_distr, sims) / len(sims_distr)
    max_pct = pcts.max(axis=1)
    pct_thr = np.maximum(max_pct - pct_delta, thr)
    # get top n over thr and within pct_delta of max where n is up to nmax
    top_n = np.maximum(1, np.minimum(nmax, (pcts > pct_thr[:, np.newaxis]).sum(axis=1)))
    top_cats = []
    for idx, nn in enumerate(top_n):
        cur_cats = [(cats[n] + (sims[idx, n], pcts[idx, n], uids[idx], n_idx))
                    for n_idx, n in enumerate(sorted_indices[idx, 0:nn])]
        top_cats.extend(cur_cats)

    df = pd.DataFrame(top_cats, columns=['category', 'cat_level', 'sim', 'pct', id_attr, 'rank'])
    if force_parents:
        groups = df.groupby(id_attr)
        extended_cats = []
        for group_id, group in groups:
            extended_cats.extend(_add_all_doc_parents_categories(group, taxonomy, group_id))
        df = pd.DataFrame(extended_cats, columns=['category', 'cat_level', 'sim', 'pct', id_attr, 'rank'])

    # walk down the taxonomic hierarchy filling in labels
    max_level = len(taxonomy) - 1
    for level in range(max_level, -1, -1):
        if level == max_level:  # leaf nodes
            df[f'level{level}'] = [cat if df.cat_level[idx] == level else None for idx, cat in enumerate(df.category)]
        elif level > 0:
            # walk up the hierarchy - get term's solution, or get solution if no term
            df[f'level{level}'] = [_get_parent(txt, level + 1, taxonomy) if txt is not None else
                                   (df.category[idx] if df.cat_level[idx] == level else None)
                                   for idx, txt in enumerate(df[f'level{level + 1}'])]
        else:       # root
            df[f'level{level}'] = [_get_parent(txt, level + 1, taxonomy) if txt is not None else
                                   df.category[idx] for idx, txt in enumerate(df[f'level{level + 1}'])]
    return df, sims_distr


def _add_all_doc_parents_categories(doc_group, taxonomy, group_id):
    """
    Adds all the parents of the tagged categories to the list of categories
    for a specific entity.

    Args:
    ------
    doc_group: pd.DataFrame
        DataFrame of categories for a specific entity.
    taxonomy: list
        List of taxonomies.
    group_id: str
        ID of the entity.

    Returns:
    --------
    group_cats: list[tuple]
        List of categories for the entity.
    """
    group_cats = doc_group.values.tolist()

    parents_to_add = set()
    current_cats = set((cat[0], cat[1]) for cat in group_cats)
    for cat in group_cats:
        if cat[1] > 0:
            lineage = get_lineage(cat[0], cat[1], taxonomy)
            for parent, parent_level in lineage:
                # Skip the parent if it's already in the list
                if (parent, parent_level) not in current_cats:
                    parents_to_add.add((parent, parent_level))
    for parent, parent_level in parents_to_add:
        group_cats.append((parent, parent_level, -1, -1, group_id, None))
    return group_cats


def _get_cat_level(cat, level_text):
    for idx, l_text in enumerate(level_text):
        if cat in l_text:
            return idx
    return -1


def compute_category_embeddings(
    cat_dict,
    level_text,
    max_workers=3
):
    logger.info("Embedding definitions")
    cats = []
    ids_texts = []
    for cat, txt in cat_dict.items():
        cat_level = _get_cat_level(cat, level_text)
        ids_texts.append((f"{cat_level}_{cat}", txt))
        cats.append((cat, cat_level))
    definition_embeddings = embed_texts_with_cache(
        ids_texts=ids_texts,
        use_cached_result=True,
        return_flat=True,
        max_workers=max_workers,
    )
    return definition_embeddings, cats


def get_entity_categories(
    cft_df,
    taxonomy,
    id_attr,
    name_attr,
    nmax,
    thr,
    pct_delta,
    entity_embeddings,
    max_level=None,
    max_workers=3,
    force_parents=False
):

    # set indices so can lookup parent categories in the hierarchy
    for tx in taxonomy[1:]:
        tx['data'].set_index(tx['name'], drop=False, inplace=True)
    # get text at different taxonomic levels
    all_text = dict()
    level_text = []
    for tx in taxonomy:
        txt_dict = dict(zip(tx['data'][tx['name']], tx['data'][tx['textattr']]))
        all_text |= txt_dict
        level_text.append(txt_dict)
    # match up to nmax categories
    # compute category embedings
    definition_embeddings, cats = compute_category_embeddings(
        all_text,
        level_text,
        max_workers=max_workers
    )
    # compute similarities between entity and category embeddings
    similarities = entity_embeddings @ definition_embeddings.T
    # get best matching caegories for each entity
    logger.info("Assigning categories")
    rdf, sims_distr = _get_similarity_percentiles(
        similarities,
        cats,
        id_attr,
        cft_df[id_attr].values,
        nmax,
        thr,
        pct_delta,
        taxonomy,
        force_parents=force_parents
    )

    # final category is set to max category level
    rdf['mapped_category'] = rdf.category
    if max_level and max_level > 0:
        attr = f"level{max_level}"
        mask = rdf.cat_level > max_level
        rdf.loc[mask, 'mapped_category'] = rdf.loc[mask, attr]

    all_df = cft_df.merge(rdf, on=id_attr)
    all_df = distribute_entity_funding(all_df, id_attr)
    return all_df, sims_distr


def distribute_entity_funding(df, id_attr):
    logger.info("Distributing entity funding across categories")
    if 'FundingFrac' in df.columns:
        df.drop(columns='FundingFrac', inplace=True)

    # Add a temporary id column to count and index the number of rows for each entity
    df['temp_id'] = df[id_attr]
    cnts = df.groupby(id_attr)["temp_id"].count()
    df['FundingFrac'] = 1 / df[id_attr].apply(lambda x: cnts[x])
    df.drop(columns='temp_id', inplace=True)
    return df


def _filter_to_leaf_nodes(df: pd.DataFrame, id_attr: str, name_attr: str, n_levels=4):
    """ assess whether there's a leaf node and higher level branches
    keeps the lower level branches
    recalculates funding fraction
    """
    filtered_rows = []
    for id_, group in df.groupby(id_attr):
        group = group.sort_values(by='cat_level', ascending=False)

        # Keep track of branches we've already added
        added_branches = set()

        # Iterate through the rows and keep the most specific assignments
        for index, row in group.iterrows():
            branch = tuple([row[f'level{i}'] for i in range(n_levels)])
            current_branch = tuple(branch[:row['cat_level'] + 1])

            # Check if a more specific branch already exists
            is_more_specific = any(current_branch == added[:len(current_branch)] for added in added_branches)

            if not is_more_specific:
                filtered_rows.append(row)
                added_branches.add(current_branch)

    df_filtered = pd.DataFrame(filtered_rows).drop_duplicates()
    df_filtered = distribute_entity_funding(df_filtered, id_attr)
    return df_filtered


def redistribute_funding_fracs(
    df,
    taxonomy=None,
    max_level=2,
    funding_attrs=[],
    keepcols=['Organization', 'P_vs_V'],
    id_attr='uid',
    across_levels=False,
    clear_duplicates=True
):
    """
    If across_levels is False, fill in missing lower level values to assign funding to a
    "No level" category so mapping is complete at all levels

    If across_levels is True, distribute funding assigned to higher level nodes
    across lower-level (child) taxonomy nodes.
    If 'taxonomy' is given, funding is distributed across all child nodes, if not
    then it is only distributed to nodes that appear in the mapping results df.
    If an entity is originally mapped for example to level 1 and max_level is 2,
    rows will be added for level 2 taxonomy nodes and fracrion of 1 / n where n
    is the number of level 2 nodes will be applied.

    Parameters
    ----------
    df : pandas.DataFrame
        Raw results of taxonomy mapping potentially with multiple rows per entity.
    taxonomy : list
        List of data at each level in multilevel taxonomy. If None then use the taxonomy nodes
        that occur in the input dataframe
    max_level : int, optional
        Max level to redsitbute to. The default is 2.
    funding_attrs : list, optional
        List of attributes to compute distributed fuding amounts on. The default is [].
    keepcols : list, optional
        List of column names of entity attributes to keep in the output.
        The default is ['Organization', 'P_vs_V'].
    id_attr : string, optional
        Name of entity id attribute column. The default is 'uid'.
    across_levels: bool, optional
        If True map lower level to higher levels; if False assign fraction to 'No_Leveln'
    merge_duplicates: bool, optional
        If True set FundingFrac to zero in all rows but one entities with rows that are duplicates
        below max_level; if False keep multiple rows with non-zero FundingFrac

    Returns
    -------
    final_df : pandas.DataFrame
        Dataframe with funding fractions, optionally computed funding amounts,
        and transferred entity attributes.

    """

    # get levels in the dataset
    levels = []
    all_levels = []
    top_lev = None
    for idx in range(0, 10):
        lev = f'level{idx}'
        if lev in df:
            if idx <= max_level:
                levels.append(lev)
            else:
                df[lev] = df[lev].fillna('')
            all_levels.append(lev)
            top_lev = idx
        else:
            break
    if top_lev < max_level:
        logger.error(f'Max level {max_level} not in the supplied dataframe')
        return None

    # set up funding attribute
    frac_attr = 'FundingFrac'
    # if no FundingFrac, add one
    if frac_attr not in df:
        distribute_entity_funding(df, id_attr)

    # preserve the original
    df = df.copy()
    # 'merge' (set all but one row to zero fraction and renormalize) when two rows have the same values below max_level
    if clear_duplicates and max_level < top_lev:
        # make sure na values are last so kept value has a max_level value
        df.sort_values([id_attr, f'level{max_level + 1}'])
        # find and zero=out FundingFraction of duplicated values
        duplicated = df.duplicated(levels + [id_attr])
        df.loc[duplicated, 'FundingFrac'] = 0
        # normalize the FundingFrac values in each entity which had duplicate level data
        dup_ids = set(df.loc[duplicated, id_attr].unique())
        dup_df = df[df[id_attr].isin(dup_ids)]
        for uid, edf in dup_df.groupby(id_attr):
            df.loc[edf.index, 'FundingFrac'] /= df.loc[edf.index, 'FundingFrac'].sum()
    if not across_levels:
        # get entities with empty level assignments
        row_with_na = df[levels].isna().sum(axis=1) > 0
        uid_with_na = set(df[id_attr][row_with_na].unique())
        na_df = df[df[id_attr].isin(uid_with_na)]
        for uid, edf in na_df.groupby(id_attr):
            # for each entity with level_n == na, see if it matches another row in the entity
            na_rows = edf.loc[edf[levels].isna().sum(axis=1) > 0]
            for idx, row in na_rows.iterrows():
                # get matches at each (non-na) level
                match_df = edf[levels].copy()
                for attr in levels:
                    if not type(row[attr]) is str:
                        break
                    match_df = match_df[match_df[attr] == row[attr]]
                if len(match_df) > 1:   # current row (with na's) matches more than itself
                    df.loc[idx, 'FundingFrac'] = 0
            # normalize the FundingFrac values
            df.loc[edf.index, 'FundingFrac'] /= df.loc[edf.index, 'FundingFrac'].sum()
        # set the na level entries to "No_level_n"
        for idx, attr in enumerate(levels):
            mask = df[attr].isna()
            if idx > 0:
                df.loc[mask, attr] = f'No_Level_{idx}_' + df.loc[mask, levels[idx - 1]]
            else:
                df.loc[mask, attr] = f'No_Level_{idx}'
        _total_df = df
    else:
        def _get_level_fracs(taxonomy, level):
            # helper fn to get taxonomy node names and fractions at a given level
            #
            # map taxonomy names to generic (leveln) names used in the mapped data
            levs = {tx['name']: f'level{idx}' for idx, tx in enumerate(taxonomy[0:level + 1])}
            # rename columns in the taxonomy data to use the generic level names
            df = taxonomy[level]['data'].rename(columns=levs)
            # get list of levels
            levels = list(levs.values())
            # level m is one level up from current (n) level
            lm = levels[level - 1]
            # compute fractions
            lm_counts = df[lm].value_counts()
            df['Fractions'] = 1 / df[lm].map(lm_counts)
            return df[levels + ['Fractions']].set_index(levels)

        if max_level < 1 or max_level > 2:
            logger.error("max_level must be 1 or 2")
            return None

        if max_level >= 1:
            # first distribute level0 (pillar-level) funding across level1 (subpillars)
            #
            # compute subpillar fractions of each pillar
            # get orgs with solutions and subpillars
            l1_df = df[df.cat_level != 0]
            # compute funding fractions of each pillar+subpillar
            l1f_df = pd.DataFrame(l1_df.groupby(['level0', 'level1'])[frac_attr].sum())
            if taxonomy is not None:     # if taxonomy is given, redistribute across all chold nodes
                l1f_df = l1f_df.join(_get_level_fracs(taxonomy, 1), how='outer').fillna(0).reset_index()
            else:           # otherwise distribute across child nodes that are in the data
                l1f_df.reset_index(inplace=True)
                l0_tots = l1f_df.level0.map(l1f_df.groupby('level0')[frac_attr].count())
                l1f_df['Fractions'] = 1 / l0_tots

            # get orgs with only pillars
            missing_l1_df = df[df.cat_level == 0]
            # distribute pillar fraction across it's subpillars based on observed subpillar fractions
            mapped_frac = (missing_l1_df.level0.apply(lambda x: l1f_df[l1f_df.level0 == x]['Fractions'].values)
                           * missing_l1_df[frac_attr])
            mapped_l1 = missing_l1_df.level0.apply(lambda x: l1f_df[l1f_df.level0 == x]['level1'].values)
            mapped_l0 = missing_l1_df.level0.apply(lambda x: l1f_df[l1f_df.level0 == x]['level0'].values)
            mapped_index = mapped_frac.explode().index
            mapped_df = pd.DataFrame({'level0': mapped_l0.explode().values,
                                      'level1': mapped_l1.explode().values,
                                      frac_attr: mapped_frac.explode().values,
                                      'cat_level': missing_l1_df.cat_level.loc[mapped_index].values,
                                      id_attr: missing_l1_df[id_attr].loc[mapped_index].values
                                      })
            total_l1_df = pd.concat([l1_df[[id_attr, 'cat_level', frac_attr] + all_levels], mapped_df])
            total_l1_df = total_l1_df.reset_index(drop=True)

            if max_level >= 2:
                # distribute subpillar-level funding across solutions
                #
                # compute soln fractions of each subpillar
                # get orgs with solutions
                l2_df = total_l1_df[~total_l1_df.level2.isna()]
                l1_with_l2 = l2_df.level1.unique()
                # compute funding fractions of each soln (level 2)
                l2f_df_ = pd.DataFrame(l2_df.groupby(levels)[frac_attr].sum())
                if taxonomy is not None:
                    fracs_df = _get_level_fracs(taxonomy, 2)
                    l2f_df = l2f_df_.join(fracs_df, how='outer').fillna(0).reset_index()
                else:
                    l2f_df = l2f_df_.reset_index()
                    l1_tots = l2f_df.level1.map(l2f_df.groupby('level1')[frac_attr].count())
                    l2f_df['Fractions'] = 1 / l1_tots

                no_l2_df = total_l1_df[total_l1_df.level2.isna()]
                mask = no_l2_df.level1.isin(l1_with_l2)
                missing_l2_df = no_l2_df[mask]  # these l1 values have no l2 assignment but there is l2 data in the taxonomy
                no_l2_df = no_l2_df[~mask]  # these level1 values are leaf nodes, there is no level 2 in this part of the taxonomy
                # distribute subpillar fractions across it's solns based on observed soln fractions
                l2_mapped_frac = (missing_l2_df.level1.apply(lambda x: l2f_df[l2f_df.level1 == x]['Fractions'].values)
                                  * missing_l2_df[frac_attr])
                l2_mapped_l2s = missing_l2_df.level1.apply(lambda x: l2f_df[l2f_df.level1 == x]['level2'].values)
                l2_mapped_l1s = missing_l2_df.level1.apply(lambda x: l2f_df[l2f_df.level1 == x]['level1'].values)
                l2_mapped_l0s = missing_l2_df.level1.apply(lambda x: l2f_df[l2f_df.level1 == x]['level0'].values)
                mapped_index = l2_mapped_frac.explode().index
                l2_mapped_df = pd.DataFrame({'level0': l2_mapped_l0s.explode().values,
                                             'level1': l2_mapped_l1s.explode().values,
                                             'level2': l2_mapped_l2s.explode().values,
                                             frac_attr: l2_mapped_frac.explode().values,
                                             'cat_level': missing_l2_df.cat_level.loc[mapped_index].values,
                                             id_attr: missing_l2_df[id_attr].loc[mapped_index].values})
                _total_df = pd.concat([l2_df[[id_attr, 'cat_level', frac_attr] + all_levels],
                                       l2_mapped_df, no_l2_df])
            else:
                _total_df = total_l1_df
        else:
            _total_df = df
        
    # for each uid, group togather fractions with same soln level
    total_grps = _total_df.groupby([id_attr, 'cat_level'] + levels)
    parts = [total_grps[frac_attr].sum()]
    # and add concatenated high-level terms
    for lev in all_levels:
        if lev not in levels:
            parts.append(total_grps[lev].apply(list))
    total_df = pd.concat(parts, axis=1).reset_index()
    # merge in the entity attributes of interest
    entity_attrs = funding_attrs + keepcols
    entity_df = df.set_index(id_attr)[entity_attrs].reset_index().drop_duplicates()
    final_df = total_df.merge(entity_df, on=id_attr)
    for attr in funding_attrs:
        final_df[attr] = final_df[attr] * final_df[frac_attr]
    return final_df


def redistribute_funding(df, funding_attr, pv='Philanthropy vs Venture'):
    # distribute pillar-level funding across subpillars

    # compute subpillar fractions of each pillar
    # get orgs with solutions and subpillars
    ssp_df = df[df.cat_level != 0]
    # compute funding fractions of each pillar+subpillar
    l1_df = pd.DataFrame(ssp_df.groupby(['level0', 'level1', pv])[funding_attr].sum().reset_index())
    l0_tots = l1_df.level0.map(l1_df.groupby('level0')[funding_attr].sum())
    l1_df['Fractions'] = l1_df[funding_attr] / l0_tots

    # get orgs with only pillars
    l0_df = df[df.cat_level == 0]
    # distribute pillar funding across it's subpillars based on observed subpillar fractions
    mapped_funding = l0_df.level0.apply(lambda x: l1_df[l1_df.level0 == x]['Fractions'].values) * l0_df[funding_attr]
    mapped_l1 = l0_df.level0.apply(lambda x: l1_df[l1_df.level0 == x]['level1'].values)
    mapped_l0 = l0_df.level0.apply(lambda x: l1_df[l1_df.level0 == x]['level0'].values)
    mapped_pv = l0_df.level0.apply(lambda x: l1_df[l1_df.level0 == x][pv].values)
    mapped_df = pd.DataFrame({'level0': mapped_l0.explode().values,
                              'level1': mapped_l1.explode().values,
                              pv: mapped_pv.explode().values,
                              funding_attr: mapped_funding.explode().values})
    total_l1_df = pd.concat([ssp_df[['level0', 'level1', 'level2', pv, funding_attr]], mapped_df])

    # distribute subpillar-level funding across solutions

    # compute soln fractions of each subpillar
    # get orgs with solutions
    l2_df = total_l1_df[~total_l1_df.level2.isna()]
    l1_with_l2 = l2_df.level1.unique()
    # compute funding fractions of each soln (level 2)
    l2f_df = pd.DataFrame(l2_df.groupby(['level0', 'level1', 'level2', pv])[funding_attr].sum().reset_index())
    l1_tots = l2f_df.level1.map(l2f_df.groupby('level1')[funding_attr].sum())
    l2f_df['Fractions'] = l2f_df[funding_attr] / l1_tots

    no_l2_df = total_l1_df[total_l1_df.level2.isna()]
    mask = no_l2_df.level1.isin(l1_with_l2)
    missing_l2_df = no_l2_df[mask]  # these l1 values have no l2 assignment but there is l2 data in the taxonomy
    no_l2_df = no_l2_df[~mask]  # these level1 values are leaf nodes, there is no level 2 in this part of the taxonomy
    # distribute subpillar funding across it's solns based on observed soln fractions
    l2_mapped_funding = (missing_l2_df.level1.apply(lambda x: l2f_df[l2f_df.level1 == x]['Fractions'].values)
                         * missing_l2_df[funding_attr])
    l2_mapped_l2s = missing_l2_df.level1.apply(lambda x: l2f_df[l2f_df.level1 == x]['level2'].values)
    l2_mapped_l1s = missing_l2_df.level1.apply(lambda x: l2f_df[l2f_df.level1 == x]['level1'].values)
    l2_mapped_l0s = missing_l2_df.level1.apply(lambda x: l2f_df[l2f_df.level1 == x]['level0'].values)
    l2_mapped_pv = missing_l2_df.level1.apply(lambda x: l2f_df[l2f_df.level1 == x][pv].values)
    l2_mapped_df = pd.DataFrame({'level0': l2_mapped_l0s.explode().values,
                                 'level1': l2_mapped_l1s.explode().values,
                                 'level2': l2_mapped_l2s.explode().values,
                                 pv: l2_mapped_pv.explode().values,
                                 funding_attr: l2_mapped_funding.explode().values})
    total_df = pd.concat([l2_df[['level0', 'level1', 'level2', pv, funding_attr]], l2_mapped_df, no_l2_df])
    return total_df


def add_mapping_to_orgs(
    org_df,
    map_df,
    id_attr,
    pct='pct',
    sim='sim',
    cats=['mapped_category', 'cat_level', 'level0', 'level1', 'level2', 'level3']
):
    def _set_with_str(series):
        return list(set([x for x in series if x]))

    temp_df = map_df[[id_attr, pct, sim] + cats]

    agg_dict = {
        pct: pd.Series.idxmax,
    }
    for cat in cats:
        if cat.startswith('level'):
            agg_dict[cat] = _set_with_str

    temp_df_agged = temp_df.groupby(id_attr).agg(agg_dict).reset_index()

    # get top category and all the columns in `cats`
    top_catgory = temp_df.loc[temp_df_agged[pct]]

    # Now get the list of the unique matched levels
    temp_df_agged.pop(pct)
    # Need to rename the columns to avoid conflicts with the top_category level names
    rename_dict = {cat: f'all_{cat}' for cat in cats if cat.startswith('level')}
    temp_df_agged.rename(
        columns=rename_dict,
        inplace=True
    )

    # Merge the top category data
    org_df_w_mapping = org_df.merge(temp_df_agged, on=id_attr, how='left').copy()

    # Merge the all category data
    org_df_w_mapping = org_df_w_mapping.merge(top_catgory, on=id_attr, how='left').copy()
    return org_df_w_mapping


def threshold_taxonomy_mapping(df, thr):
    # keep entity if its mean pct is above the threshold
    keep = df.groupby('uid')['pct'].mean() > thr
    # turn it into a set for quick comparison
    keep_uids = set(keep.index[keep].values)
    # do the filtering
    return df[df.uid.isin(keep_uids)]


def threshold_taxonomy_mapping_similarity(df, sim_thr):
    # keep entity if its mean pct is above the threshold
    keep = df.groupby('uid')['sim'].mean() > sim_thr
    # turn it into a set for quick comparison
    keep_uids = set(keep.index[keep].values)
    # do the filtering
    return df[df.uid.isin(keep_uids)]


def run_fewshot_classification(
    all_df,
    id_col,
    text_col,
    taxonomy,
    reranked_relevancy_col='reranked_relevancy',
    use_cached_results=True,
    max_workers=3,
    max_errors=1,
    mapped_category_col='mapped_category',
    examples_dict=None
):
    definitions_dict = dict([
        category_definition_tuple
        for tax_level in taxonomy
        for category_definition_tuple in
        tax_level['data'][[tax_level['name'], tax_level['textattr']]].values.tolist()
    ])

    ids_to_payloads = {}
    all_df['taxonomy_mapping_id'] = all_df.apply(lambda x: f"{x[id_col]}_{x[mapped_category_col]}", axis=1)
    for _, row in all_df.iterrows():

        category = row[mapped_category_col]
        description = definitions_dict[category].strip()

        _id = row["taxonomy_mapping_id"]

        entity_activity_dict = {
            "entity_description": row[text_col],
            "activity_name": category,
            "activity_description": description,
        }

        # TODO: Explore how to have custom examples
        ids_to_payloads[_id] = {
            "entity_activity_dict": entity_activity_dict,
            "examples_dicts": examples_dict,
        }

    with get_session() as session:
        few_shot_cache = FewShotCache(session=session)
        ids_to_responses = few_shot_cache.bulk_get_cache_or_run(
            given_ids_texts=ids_to_payloads.items(),
            use_cached_result=use_cached_results,
            max_workers=max_workers,
            max_errors=max_errors,
        )
        ids_to_label = {k: v['is_relevant'] for k, v in ids_to_responses.items()}
    all_df[reranked_relevancy_col] = all_df['taxonomy_mapping_id'].map(ids_to_label)
    return all_df
