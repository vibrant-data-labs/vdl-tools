from collections import Counter, defaultdict
from dataclasses import dataclass

from dataclasses_json import dataclass_json
import pandas as pd

from vdl_tools.shared_tools.web_summarization.page_choice.constants import PATHS_TO_KEEP
from vdl_tools.shared_tools.tools.unique_ids import create_deterministic_md5

@dataclass_json
@dataclass
class PageType:
    name: str
    value: list[str] | str


@dataclass_json
@dataclass
class ScrapedPageRecord:
    type: PageType
    source: str
    subpath: str
    text: str


def deterministic_len_key(a_string):
    return (len(a_string), create_deterministic_md5(a_string))


def filter_links(
    links: list[str],
    keep_paths: tuple[str] = PATHS_TO_KEEP,
    max_per_subpath: int = 8,
):
    """
    Filter the internal links on a site

    Only consider pages that contain any of the keep_paths words anywhere in the path.

    For each top-level path, keep the top 4 bottom pages that contain that path as their top-level path.

    For example, if an organization had the following pages listed:

    [
        "/",
        "/about-us",
        "/about-us/news",
        "/about-us/news/our-work-in-africa",
        "/about-us/news/our-work-in-asia",
        "/about-us/news/our-work-in-europe",
        "/about-us/news/our-work-in-north-america",
        "/about-us/news/our-work-in-south-america",
        "/privacy-policy",
    ]

    keep_paths = {"about-us", "news"}
    max_per_subpath = 4

    The final set would be
    [
        "/",
        "/about-us",

        "/about-us/news/our-work-in-africa",
        "/about-us/news/our-work-in-asia",
        "/about-us/news/our-work-in-europe",
        "/about-us/news/our-work-in-north-america",
    ]

    Note that the following pages were not kept:

        # It's a middle path
        "/about-us/news",
        # 4 pages were already kept for this top-level path
        "/about-us/news/our-work-in-south-america",
        # It's not in keep_paths
        "/privacy-policy",


    Parameters
    ----------
    org_pages_records : list[dict]
        A list of dictionaries with the keys:
        [
            "type",
            "source",
            "subpath",
            "text",        
        ]
    keep_paths : set[str]
        Set of words that must exist somewhere in the path
    max_per_subpath : int, optional
        Maximium number of bottom pages to keep for each top level page, by default 4

    Returns
    -------
    list[str]
        List of the paths to keep for the organization
    """
    if not keep_paths:
        return links

    # Filter duplicates if any
    keep_paths = set(keep_paths)
    links = [x.strip('/') for x in set(links)]

    def _link_part_in_keep_paths(link_parts, keep_paths):
        return len(set(link_parts).intersection(keep_paths)) > 0

    org_paths_split = []
    for link in links:
        link_split = link.split("/")
        if link in {"", "/"}:
            org_paths_split.append(link_split)
        elif _link_part_in_keep_paths(link_split, keep_paths):
            org_paths_split.append(link_split)

    # Get a count of how many children each path could have (to get top and bottom level paths)
    org_paths_counter = Counter()
    for split_path in org_paths_split:
        for i in range(1, len(split_path)+1):
            org_paths_counter[tuple(split_path[:i])] += 1

    # Only keep as candidates those that are top level or bottom level
    top_level_candidates, bottom_level_candidates = [], []
    for path, count in org_paths_counter.most_common(1000):
        if len(path) == 1:
            top_level_candidates.append((path, count))
        elif count == 1:
            bottom_level_candidates.append((path, count))

    org_pages_dict = defaultdict(list)
    for candidate, count in top_level_candidates:
        top_path = "/".join(candidate)
        org_pages_dict[top_path].append(top_path)
        for bottom_candidate, count in bottom_level_candidates:
            if (
                bottom_candidate[0] == candidate[0] and
                len(org_pages_dict[top_path]) <= max_per_subpath
            ):
                bottom_path = "/".join(bottom_candidate)
                org_pages_dict[top_path].append(bottom_path)
    return [page for page_list in org_pages_dict.values() for page in page_list]


def choose_org_pages(
    org_pages_records: list[ScrapedPageRecord],
    keep_paths: tuple[str] = PATHS_TO_KEEP,
    max_per_subpath: int = 4
):
    """Pick the pages to keep for an organization
    """
    # FIXME: (ztawil) This is a hack to make this work with the current code but it should be fixed to use the class attrs
    if isinstance(org_pages_records[0], ScrapedPageRecord):
        org_pages_records = [x.to_dict() for x in org_pages_records]

    if not keep_paths:
        return org_pages_records

    # We hash the text so need to be very determininstic when sorting
    subpaths = [x['subpath'].strip("/") for x in org_pages_records]
    subpaths = sorted(subpaths, key=deterministic_len_key)
    print(subpaths)
    return filter_links(subpaths, keep_paths, max_per_subpath)


def filter_pages(
    df: pd.DataFrame | list[ScrapedPageRecord],
    keep_paths: set[str] = set(PATHS_TO_KEEP),
    max_per_subpath: int = 4,
) -> pd.DataFrame:
    """Takes a dataframe of scraped pages and filters it down to the pages we want to keep.

    Parameters
    ----------
    df : DataFrame
        A dataframe with the expected columns:
        [
            "type",
            "source",
            "subpath",
            "text",        
        ]
    keep_paths : set[str]
        A set of words that must exist somewhere in the path
    max_per_subpath : int, optional
        The maximum number of bottom pages that can exist for a top page, by default 4

    Returns
    -------
    DataFrame
        A dataframe filtered down to the pages we want to keep
    """
    if isinstance(df, list):
        df = pd.DataFrame([x.to_dict() for x in df])

    df['subpath'] = df['subpath'].apply(lambda x: x.strip("/"))
    if not keep_paths:
        return df

    orgs_and_pages = {}
    for i, org_group in df.groupby("source"):
        org_pages_records = org_group.to_dict(orient="records")
        keep_pages = set(
            choose_org_pages(
                org_pages_records=org_pages_records,
                keep_paths=keep_paths,
                max_per_subpath=max_per_subpath,
            )
        )

        org_pages = []
        for page_record in org_pages_records:
            if page_record['subpath'] in keep_pages:
                org_pages.append(page_record)

        orgs_and_pages[i] = org_pages

    all_records = []
    for pages in orgs_and_pages.values():
        all_records.extend(pages)
    return pd.DataFrame(all_records)
