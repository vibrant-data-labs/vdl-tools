import pandas as pd

import vdl_tools.shared_tools.project_config as pc
import vdl_tools.shared_tools.taxonomy_mapping.taxonomy_mapping as tm
import vdl_tools.shared_tools.taxonomy_mapping.fewshot_examples as fse
from vdl_tools.shared_tools.tools.logger import logger


def add_taxonomy_mapping(
    df,
    entity_embeddings,
    taxonomy,
    id_col,
    text_col,
    name_col='Organization',
    nmax=5,
    threshold=90,
    pct_delta=1,
    run_fewshot_classification=True,
    filter_fewshot_classification=True,
    fewshot_examples=None,
    use_cached_results=True,
    max_workers=3,
    force_parents=True,
    distribute_funding=True,
    mapping_name=None,
    max_distr_funding_level=2,
):
    logger.info("Starting Taxonomy mapping for %s", mapping_name)
    if entity_embeddings is None:
        entity_embeddings = tm.get_or_compute_embeddings(
            org_df=df,
            id_col=id_col,
            text_col=text_col,
            max_workers=max_workers,
        )

    all_df, _ = tm.get_entity_categories(
        df,
        taxonomy,
        id_attr=id_col,
        name_attr=name_col,
        nmax=nmax,
        thr=threshold,
        pct_delta=pct_delta,
        entity_embeddings=entity_embeddings,
        max_workers=max_workers,
        force_parents=force_parents
    )

    if force_parents:
        # if force_parents is False, then we need to filter out the rows where the sim is -1
        # which is the indication that they were forced in
        forced_in_df = all_df[all_df['sim'] == -1].copy()
        all_df = all_df[all_df['sim'] > -1].copy()

    if run_fewshot_classification:
        all_df = tm.run_fewshot_classification(
            all_df=all_df,
            id_col=id_col,
            text_col=text_col,
            taxonomy=taxonomy,
            reranked_relevancy_col='reranked_relevancy',
            use_cached_results=use_cached_results,
            max_workers=max_workers,
            examples_dict=fewshot_examples
        )

        if force_parents:
            # Only run fewshot classification on the entities that
            # had no matches after the initial fewshot classification
            totally_unmatched_uids = (
                set(forced_in_df[id_col].unique()) -
                set(all_df[all_df['reranked_relevancy']][id_col].unique())
            )

            forced_in_df = forced_in_df[forced_in_df[id_col].isin(totally_unmatched_uids)]

            forced_in_df = tm.run_fewshot_classification(
                all_df=forced_in_df,
                id_col=id_col,
                text_col=text_col,
                taxonomy=taxonomy,
                reranked_relevancy_col='reranked_relevancy',
                use_cached_results=use_cached_results,
                max_workers=max_workers,
                examples_dict=fewshot_examples,
            )
            all_df = pd.concat([all_df, forced_in_df])

        if filter_fewshot_classification:
            filtered_all_df = all_df[all_df['reranked_relevancy'].astype(bool)].copy()

            filtered_all_df = tm.distribute_entity_funding(filtered_all_df, id_col)
            all_df = all_df.merge(
                filtered_all_df[['taxonomy_mapping_id', 'FundingFrac']],
                on='taxonomy_mapping_id',
                how='left',
                suffixes=('', '_filtered')
            )
            all_df['FundingFrac_filtered'] = all_df['FundingFrac_filtered'].fillna(0)
        else:
            filtered_all_df = all_df.copy()
    else:
        filtered_all_df = all_df.copy()

    if distribute_funding:
        # Remove this column from the output since it is calculated before re-ranking
        filtered_all_df.pop('FundingFrac')
        distributed_funding_df = tm.redistribute_funding_fracs(
            df=filtered_all_df,
            taxonomy=taxonomy,
            id_attr=id_col,
            keepcols=[name_col],
            max_level=max_distr_funding_level,
        )
    else:
        distributed_funding_df = None

    if mapping_name:
        rename = {f'level{tx["level"]}': f'level{tx["level"]}_{mapping_name}' for tx in taxonomy}
        pct = 'pct_' + mapping_name
        sim = 'sim_' + mapping_name
        rename['mapped_category'] = mapping_name
        rename['pct'] = pct
        rename['sim'] = sim
        rename['cat_level'] = 'cat_level_' + mapping_name
        filtered_all_df = filtered_all_df.rename(columns=rename)

    return filtered_all_df, distributed_funding_df


###########################################################
# OneEarth-specific functions

def validate_one_earth_taxonomy(taxonomy_path):
    taxonomy = load_one_earth_taxonomy(taxonomy_path,
                                       solution_textattr=oe_solution_textattr,
                                       )

    all_errors = []
    for i, level in enumerate(taxonomy):
        if i == 0:
            continue
        level_data = level['data']
        level_name = level['name']

        prev_level_data = taxonomy[i-1]['data']
        prev_level_name = taxonomy[i-1]['name']

        # Make sure all previous level names in the current level are present
        # in the previous level
        prev_level_names = prev_level_data[prev_level_name].unique()
        current_parent_names = level_data[prev_level_name].unique()
        missing_names = set(current_parent_names) - set(prev_level_names)

        if missing_names:
            all_errors.append(
                {
                    'level': i,
                    'prev_level': i-1,
                    'level_name': level_name,
                    'prev_level_name': prev_level_name,
                    'missing_names': missing_names
                }
            )

    if len(all_errors) > 0:
        errors_strs = []
        for error in all_errors:
            errors_strs.append(
                f"{error['missing_names']} in level {error['level']} "
                f"in column {error['prev_level_name']} are not present in level "
                f"{error['prev_level']} in column "
                f"{error['prev_level_name']}"
            )
        full_error_str = "\n".join(errors_strs)
        raise ValueError(f"Validation failed:\n{full_error_str}")


def load_netzero_taxonomy(
    taxonomy_path,
    max_depth=5,
    aggregate_extra_levels=True,
):
    taxonomy = []
    for i in range(max_depth + 1):
        df = pd.read_excel(taxonomy_path, sheet_name=f"level_{i}").drop_duplicates(subset=[f'level_{i}'])
        taxonomy.append({
            'level': i,
            'name': f"level_{i}",
            'data': df,
            'textattr': 'description'
        })

    if not aggregate_extra_levels:
        return taxonomy

    last_df = None
    current_level = max_depth + 1
    reached_max = False

    # Aggregate extra levels into a single level
    # Start with joining the last level to the next level using parent_ids
    # This will give us a mapping from the max_depth + 1 ids all the way to the last
    # Potential level in the hierarchy
    while not reached_max:
        try:
            df = pd.read_excel(taxonomy_path, sheet_name=f"level_{current_level}").drop_duplicates(subset=[f'level_{current_level}'])
        except ValueError:
            reached_max = True
            break
        if last_df is None:
            last_df = df
        else:
            last_df = last_df.merge(df, right_on='parent_id', left_on=f'id_level_{current_level - 1}', how='left', suffixes=('', f'_level_{current_level}'))
        last_df.rename(columns={
            'id': f'id_level_{current_level}',
            'description': f'description_level_{current_level}'
        }, inplace=True)
        current_level += 1

    # Now that they are all joined, we can unstack the dfs so each "level" is a new set of rows
    # We will then concatenate them all together at the end
    # Make sure the "parent_id" is taken from the max_depth level
    stacked_dfs = []
    for i in range(max_depth + 1, current_level):
        mini_df = last_df[[
            f'id_level_{i}',
            f'level_{i}',
            # Always have the column of the first level after the max depth
            'parent_id',
            f"level_{max_depth}",
            f'description_level_{i}'
        ]].copy()
        mini_df = mini_df[mini_df[f'description_level_{i}'].notnull()]

        # Rename the columns to be the max_depth + 1 level to simulate the collapsing
        # of the extra levels
        mini_df.rename(columns={
            f'level_{i}': f'level_{max_depth + 1}',
            f'description_level_{i}': 'description',
            f'id_level_{i}': 'id',
        }, inplace=True)
        stacked_dfs.append(mini_df)

    # Concatenate all the dfs together
    subterm_df = pd.concat(stacked_dfs)
    subterm_df.drop_duplicates(subset=['id'], inplace=True)
    max_depth_col = f'level_{max_depth}'
    max_depth_plus_one_col = f'level_{max_depth + 1}'
    # Rename to ensure unique names
    subterm_df[max_depth_plus_one_col] = subterm_df.apply(
        lambda x: f"{x[max_depth_col]} - {x[max_depth_plus_one_col]}",
        axis=1,
    )
    taxonomy.append({
        'level': max_depth + 1,
        'name': f'level_{max_depth + 1}',
        'data': subterm_df,
        'textattr': 'description'
    })

    return taxonomy


def load_one_earth_taxonomy(taxonomy_path,
                            add_geo_engineering=False,
                            solution_textattr='Definition'  #  or 'ExpandedText'
                            ):
    pillar_df = pd.read_excel(taxonomy_path, sheet_name="Pillars")
    sub_df = pd.read_excel(taxonomy_path, sheet_name="SubPillars")
    soln_df = pd.read_excel(taxonomy_path, sheet_name="Solutions")
    energy_term_df = pd.read_excel(taxonomy_path, sheet_name="Energy").ffill()
    ag_term_df = pd.read_excel(taxonomy_path, sheet_name="Regenerative Ag").ffill()
    nature_term_df = pd.read_excel(taxonomy_path, sheet_name="Nature Conservation").ffill()

    # for pillars, sub, and soln exclude any rows that have Exclude == 1 if Exclude column exists
    if 'Exclude' in pillar_df.columns:
        pillar_df = pillar_df[pillar_df['Exclude'] != 1].copy()
    if 'Exclude' in sub_df.columns:
        sub_df = sub_df[sub_df['Exclude'] != 1].copy()
    if 'Exclude' in soln_df.columns:
        soln_df = soln_df[soln_df['Exclude'] != 1].copy()
    # Concatenate sub-term sheets into a single subterm dataframe
    term_df = pd.concat([energy_term_df, ag_term_df, nature_term_df])
    if add_geo_engineering:
        # add terms for geo-engineering pillar
        geo_term_df = pd.read_excel(taxonomy_path, sheet_name="Geo-Engineering").ffill()
        term_df = pd.concat([term_df, geo_term_df])

    term_df = term_df[term_df['Exclude'] != 1].copy()
    term_df.drop(columns=['Exclude'], inplace=True)
    # subterms must be unique but raw subterms are sometimes repeated in different solutions
    term_df['Sub-Term'] = term_df['Sub-Term'] + ' (' + term_df['Solution'] + ')'

    taxonomy = [
        {'level': 0, 'name': 'Pillar', 'data': pillar_df, 'textattr': 'Definition'},
        {'level': 1, 'name': 'Sub-Pillar', 'data': sub_df, 'textattr': 'Definition'},
        {'level': 2, 'name': 'Solution', 'data': soln_df, 'textattr': solution_textattr},
        {'level': 3, 'name': 'Sub-Term', 'data': term_df, 'textattr': 'Sub-Term Definition'}
    ]
    return taxonomy


def load_one_earth_intersectional(taxonomy_path):
    it_df = pd.read_excel(taxonomy_path, sheet_name="Intersec.Theme").ffill()
    it_df['Sub-Term'] = it_df['Name'] + ': ' + it_df['Sub-Term']
    it0_df = it_df[['Name', 'Definition']].drop_duplicates()
    taxonomy = [
        {'level': 0, 'name': 'Name', 'data': it0_df, 'textattr': 'Definition'},
        {'level': 1, 'name': 'Sub-Term', 'data': it_df, 'textattr': 'Sub-Term Definition'},
        ]
    return taxonomy


def load_one_earth_falsesolns(taxonomy_path):
    fs_df = pd.read_excel(taxonomy_path, sheet_name="False Solutions").ffill()
    fs_df['Sub-Term'] = fs_df['Solution'] + ': ' + fs_df['Sub-Term']
    fs0_df = fs_df[['Solution', 'Definition']].drop_duplicates()
    taxonomy = [
        {'level': 0, 'name': 'Solution', 'data': fs0_df, 'textattr': 'Definition'},
        {'level': 1, 'name': 'Sub-Term', 'data': fs_df, 'textattr': 'Sub-Term Definition'},
        ]
    return taxonomy


def load_one_earth_levers(taxonomy_path):
    loc_df = pd.read_excel(taxonomy_path, sheet_name="LOC").ffill()
    loc0_df = loc_df[['Name', 'Definition']].drop_duplicates()
    taxonomy = [
        {'level': 0, 'name': 'Name', 'data': loc0_df, 'textattr': 'Definition'},
        ]
    return taxonomy


def load_one_earth_hierarchical_levers(oe_levers_path):
    l0_df = pd.read_excel(oe_levers_path, sheet_name="Level0").ffill()
    l1_df = pd.read_excel(oe_levers_path, sheet_name="Level1").ffill()
    l2_df = pd.read_excel(oe_levers_path, sheet_name="Level2").ffill()
    taxonomy = [
        {'level': 0, 'name': 'Level0', 'data': l0_df, 'textattr': 'Definition'},
        {'level': 1, 'name': 'Level1', 'data': l1_df, 'textattr': 'Definition'},
        {'level': 2, 'name': 'Level2', 'data': l2_df, 'textattr': 'Definition'},
        ]
    return taxonomy


def add_one_earth_taxonomy(
    df,
    id_col,
    text_col,
    name_col='Organization',
    run_fewshot_classification=True,
    filter_fewshot_classification=True,
    use_cached_results=True,
    paths=None,
    max_workers=3,
    force_parents=True,
    add_intersectional=True,
    add_falsesolns=True,
    add_geo_engineering=False,
    add_levers_of_change=True,
    mapping_name="one_earth_category",
    taxonomy_path=None,
    oe_solution_textattr='Definition',  # or "ExpandedText"
    results_path=None,
    distributed_funding_results_path=None,
    levers_path=None,
    levers_results_path=None,
    max_depth=2,
):
    paths = paths or pc.get_paths()
    taxonomy_path = taxonomy_path or paths["one_earth_taxonomy"]
    results_path = results_path or paths["one_earth_taxonomy_mapping_results"]
    distributed_funding_results_path = distributed_funding_results_path or paths["oe_tax_mapping_distributed_funding_results"]
    levers_path = levers_path or paths["one_earth_levers"]
    levers_results_path = levers_results_path or paths["one_earth_taxonomy_levers_results"]

    if filter_fewshot_classification and not run_fewshot_classification:
        raise ValueError("Cannot filter few shot classification if it is not run")

    entity_embeddings = tm.get_or_compute_embeddings(
        org_df=df,
        id_col=id_col,
        text_col=text_col,
        max_workers=max_workers
    )

    taxonomy = load_one_earth_taxonomy(taxonomy_path,
                                       solution_textattr=oe_solution_textattr,
                                        add_geo_engineering=add_geo_engineering,
                                       )

    # add main taxonomy mapping
    all_df, distr_df = add_taxonomy_mapping(
        df,
        entity_embeddings,
        taxonomy,
        id_col,
        text_col,
        name_col=name_col,
        run_fewshot_classification=run_fewshot_classification,
        filter_fewshot_classification=filter_fewshot_classification,
        fewshot_examples=None,
        use_cached_results=use_cached_results,
        force_parents=force_parents,
        mapping_name=mapping_name,
        max_distr_funding_level=max_depth,
    )

    # reduce the number of columns in the output
    original_columns = set(df.columns)
    # Keep all the new columns
    new_columns = list(all_df.columns.difference(original_columns))
    keep_columns = [id_col, name_col, text_col] + new_columns
    all_df[keep_columns].to_json(results_path, orient='records')
    if distr_df is not None:
        # make directory if it doesn't exist
        distributed_funding_results_path.parent.mkdir(parents=True, exist_ok=True)
        distr_df.to_json(distributed_funding_results_path, orient='records')

    if mapping_name:
        pct = 'pct_' + mapping_name
        sim = 'sim_' + mapping_name
        cols = [mapping_name, f'cat_level_{mapping_name}'] + [f'level{tx["level"]}_{mapping_name}' for tx in taxonomy]
    else:
        pct = 'pct'
        sim = 'sim'
        cols = ['mapped_category', 'cat_level'] + [f'level{tx["level"]}' for tx in taxonomy]
    new_df = tm.add_mapping_to_orgs(df, all_df, id_col, pct=pct, sim=sim, cats=cols)

    if add_intersectional:
        # add intersectional themes mapping
        mapping_name = "Intersectional"
        pct = 'pct_' + mapping_name
        sim = 'sim_' + mapping_name
        it_taxonomy = load_one_earth_intersectional(taxonomy_path)
        it_all_df, _ = add_taxonomy_mapping(
            df,
            entity_embeddings,
            it_taxonomy,
            id_col,
            text_col,
            name_col=name_col,
            run_fewshot_classification=run_fewshot_classification,
            filter_fewshot_classification=filter_fewshot_classification,
            fewshot_examples=fse.intersectional_fewshot_examples,
            use_cached_results=use_cached_results,
            max_workers=3,
            force_parents=False,
            distribute_funding=False,
            mapping_name=mapping_name
        )
        new_df = tm.add_mapping_to_orgs(new_df, it_all_df, id_col, pct=pct, sim=sim,
                                        cats=[mapping_name, f'level0_{mapping_name}', f'level1_{mapping_name}'])

    if add_falsesolns:
        # add false solutions mapping
        mapping_name = "FalseSolns"
        pct = 'pct_' + mapping_name
        sim = 'sim_' + mapping_name
        fs_taxonomy = load_one_earth_falsesolns(taxonomy_path)
        fs_all_df, _ = add_taxonomy_mapping(
            df,
            entity_embeddings,
            fs_taxonomy,
            id_col,
            text_col,
            name_col=name_col,
            run_fewshot_classification=run_fewshot_classification,
            filter_fewshot_classification=filter_fewshot_classification,
            fewshot_examples=fse.falsesolns_fewshot_examples,
            use_cached_results=use_cached_results,
            max_workers=3,
            force_parents=False,
            distribute_funding=False,
            mapping_name=mapping_name
        )
        new_df = tm.add_mapping_to_orgs(new_df, fs_all_df, id_col, pct=pct, sim=sim,
                                        cats=[mapping_name, f'level0_{mapping_name}', f'level1_{mapping_name}'])

    if add_levers_of_change:
        # add levers of change mapping
        mapping_name = "Levers"
        pct = 'pct_' + mapping_name
        sim = 'sim_' + mapping_name
        loc_taxonomy = load_one_earth_hierarchical_levers(levers_path)
        loc_all_df, _ = add_taxonomy_mapping(
            df,
            entity_embeddings,
            loc_taxonomy,
            id_col,
            text_col,
            name_col=name_col,
            run_fewshot_classification=run_fewshot_classification,
            filter_fewshot_classification=filter_fewshot_classification,
            fewshot_examples=fse.falsesolns_fewshot_examples,
            use_cached_results=use_cached_results,
            max_workers=3,
            force_parents=False,
            distribute_funding=False,
            mapping_name=mapping_name
        )
        cols = [mapping_name, f'cat_level_{mapping_name}'] + [f'level{tx["level"]}_{mapping_name}'
                                                              for tx in loc_taxonomy]
        # save mapping results to json
        new_columns = list(loc_all_df.columns.difference(original_columns))
        keep_columns = [id_col, name_col, text_col] + new_columns
        loc_all_df[keep_columns].to_json(levers_results_path, orient='records')
        #new_df = tm.add_mapping_to_orgs(new_df, loc_all_df, id_col, pct=pct, sim=sim,
        #                                cats=[mapping_name, f'level0_{mapping_name}'])
        new_df = tm.add_mapping_to_orgs(new_df, loc_all_df, id_col, pct=pct, sim=sim, cats=cols)

    return new_df


def add_tailwind_taxonomy(
    df,
    id_col,
    text_col,
    name_col='Organization',
    run_fewshot_classification=True,
    filter_fewshot_classification=True,
    use_cached_results=True,
    paths=None,
    max_workers=3,
    mapping_name="tailwind_category",
):

    paths = paths or pc.get_paths()
    if filter_fewshot_classification and not run_fewshot_classification:
        raise ValueError("Cannot filter few shot classification if it is not run")

    theme_df = pd.read_excel(paths["tailwind_taxonomy"], sheet_name="Themes")
    sector_df = pd.read_excel(paths["tailwind_taxonomy"], sheet_name="Sectors")
    examples_df = pd.read_excel(paths["tailwind_taxonomy"], sheet_name="Examples")

    # add prefix to the columns and combine sector-examples to be unique

    theme_df['Theme'] =  theme_df['Theme']
    sector_df['Theme'] =  sector_df['Theme']
    sector_df['Sector'] =  sector_df['Sector']
    examples_df['Theme'] =  examples_df['Theme']
    examples_df['Sector'] =  examples_df['Sector']
    examples_df['Examples'] = examples_df['Sector'] + '-' + examples_df['Examples']

    taxonomy = [
        {'level': 0, 'name': 'Theme', 'data': theme_df, 'textattr': 'Theme Definition'},
        {'level': 1, 'name': 'Sector', 'data': sector_df, 'textattr': 'revised_definition'},
        {'level': 2, 'name': 'Examples', 'data': examples_df, 'textattr': 'gpt_definition'},
        ]

    entity_embeddings = tm.get_or_compute_embeddings(
        org_df=df,
        id_col=id_col,
        text_col=text_col,
        max_workers=max_workers
    )

    # add main taxonomy mapping
    all_df, distr_df = add_taxonomy_mapping(
        df,
        entity_embeddings,
        taxonomy,
        id_col,
        text_col,
        name_col=name_col,
        nmax=5,
        threshold=90,
        pct_delta=2,
        run_fewshot_classification=run_fewshot_classification,
        filter_fewshot_classification=filter_fewshot_classification,
        fewshot_examples=None,
        use_cached_results=use_cached_results,
        mapping_name=mapping_name
    )

    all_df.to_json(paths["tailwind_taxonomy_mapping_results"], orient='records')
    # reduce the number of columns in the output
    original_columns = set(df.columns)
    # Keep all the new columns
    new_columns = list(all_df.columns.difference(original_columns))
    keep_columns = [id_col, name_col, text_col] + new_columns
    all_df[keep_columns].to_json(paths["tailwind_taxonomy_mapping_results"], orient='records')
    if distr_df is not None:
        # make directory if it doesn't exist
        paths["tw_tax_mapping_distributed_funding_results"].parent.mkdir(parents=True, exist_ok=True)
        distr_df.to_json(paths["tw_tax_mapping_distributed_funding_results"], orient='records')

    if mapping_name:
        pct = 'pct_' + mapping_name
        sim = 'sim_' + mapping_name
        cols = [mapping_name, f'cat_level_{mapping_name}'] + [f'level{tx["level"]}_{mapping_name}'
                                                              for tx in taxonomy]
    else:
        pct = 'pct'
        sim = 'sim'
        cols = ['mapped_category', 'cat_level'] + [f'level{tx["level"]}' for tx in taxonomy]

    new_df = tm.add_mapping_to_orgs(df, all_df, id_col, pct=pct, sim=sim, cats=cols)

    return new_df

def add_netzero_taxonomy(
    df,
    id_col,
    text_col,
    name_col='Organization',
    run_fewshot_classification=True,
    filter_fewshot_classification=True,
    use_cached_results=True,
    paths=None,
    max_workers=3,
    force_parents=True,
    mapping_name="netzero_category",
    max_depth=5,
    taxonomy_path=None,
    results_path=None,
    distributed_funding_results_path=None,
):
    paths = paths or pc.get_paths()
    taxonomy_path = taxonomy_path or paths["netzero_taxonomy"]
    results_path = results_path or paths["netzero_taxonomy_mapping_results"]
    distributed_funding_results_path = distributed_funding_results_path or paths["netzero_tax_mapping_distributed_funding_results"]

    if filter_fewshot_classification and not run_fewshot_classification:
        raise ValueError("Cannot filter few shot classification if it is not run")

    entity_embeddings = tm.get_or_compute_embeddings(
        org_df=df,
        id_col=id_col,
        text_col=text_col,
        max_workers=max_workers
    )

    taxonomy = load_netzero_taxonomy(taxonomy_path, max_depth=max_depth)

    # add main taxonomy mapping
    all_df, distr_df = add_taxonomy_mapping(
        df,
        entity_embeddings,
        taxonomy,
        id_col,
        text_col,
        name_col=name_col,
        run_fewshot_classification=run_fewshot_classification,
        filter_fewshot_classification=filter_fewshot_classification,
        fewshot_examples=None,
        use_cached_results=use_cached_results,
        force_parents=force_parents,
        mapping_name=mapping_name,
        max_distr_funding_level=max_depth,
    )

    # reduce the number of columns in the output
    original_columns = set(df.columns)
    # Keep all the new columns
    new_columns = list(all_df.columns.difference(original_columns))
    keep_columns = [id_col, name_col, text_col] + new_columns
    all_df[keep_columns].to_json(results_path, orient='records')
    if distr_df is not None:
        # make directory if it doesn't exist
        distributed_funding_results_path.parent.mkdir(parents=True, exist_ok=True)
        distr_df.to_json(distributed_funding_results_path, orient='records')

    if mapping_name:
        pct = 'pct_' + mapping_name
        sim = 'sim_' + mapping_name
        cols = [mapping_name, f'cat_level_{mapping_name}'] + [f'level{tx["level"]}_{mapping_name}' for tx in taxonomy]
    else:
        pct = 'pct'
        sim = 'sim'
        cols = ['mapped_category', 'cat_level'] + [f'level{tx["level"]}' for tx in taxonomy]
    new_df = tm.add_mapping_to_orgs(df, all_df, id_col, pct=pct, sim=sim, cats=cols)

    return new_df
