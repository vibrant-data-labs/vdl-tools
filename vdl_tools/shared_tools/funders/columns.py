"""Funders of each org with a stable id, so two funders with one name stay two.

A funder is a Crunchbase investor uuid (``cb:<uuid>``), a Giving Tuesday granter EIN
(``ein:<ein>``) or, for a name with no id, ``name:<slug>``. The player shows names, but a
name is not an identity: 8 EINs are filed as "SMITH FAMILY FOUNDATION". ``Funder_Ids``
lists the id of each funder in ``Funders``, in the same order.

A funder in both sources has one id of each; they are not merged.
"""
import json
import logging
import re

import numpy as np
import pandas as pd

from vdl_tools.scrape_enrich.display_name import PROMPT, PROMPT_FILED_NAME, PROMPT_SHARED_SITE, pick_display_names
from vdl_tools.shared_tools.funders.ids import normalize_ein, slugify
from vdl_tools.shared_tools.funders.names import (
    clean_funder_text,
    funder_key,
    funder_name_and_dba,
    pick_spelling,
    website_domain,
)

logger = logging.getLogger(__name__)

NAME_LIST_COLS = ["Investors", "Donors", "Funder_Names"]


def _records(value):
    """A list of dicts from a list, an array or a JSON string; ``[]`` for a blank."""
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, str):
        value = json.loads(value) if value.strip() else []
    return value if isinstance(value, list) else []


def _names(value):
    return [str(n).strip() for n in _records(value) if str(n).strip()]


def collect_funders(df):
    """One row per org and funder: ``pos`` (row position), ``funder_id``, ``name``, ``source``, ``dba``.

    ``source`` is ``investor`` (Crunchbase) or ``donor`` (Giving Tuesday and the rest); ``dba`` is
    the granter's DBA from its name lines, ``""`` for the others.
    A name in ``Investors``/``Donors``/``Funder_Names`` that no ``Investors Data`` or ``granters``
    record accounts for is another spelling of a funder when exactly one funder id has its
    ``funder_key`` (on this org first, then anywhere); otherwise it gets a ``name:`` id. A name that
    is only the start of a funder the org already has ("Community Foundation" for "Community
    Foundation of Greater Memphis") is the cut-off first line of it and is dropped.
    """
    out, leftovers = [], []
    for pos, row in enumerate(df.to_dict("records")):
        covered = set()
        for rec in _records(row.get("Investors Data")):
            name = clean_funder_text(rec.get("name"))
            if rec.get("uuid") and name:
                out.append((pos, f"cb:{rec['uuid']}", name, "investor", ""))
                covered.add(str(rec.get("name")).strip())
        for rec in _records(row.get("granters")):
            ein = normalize_ein(rec.get("ein"))
            if ein is None:
                continue
            name, dba = funder_name_and_dba(rec.get("businessname1"), rec.get("businessname2"), rec.get("dba_name"))
            out.append((pos, f"ein:{ein}", name or ein, "donor", dba))
            covered.update({str(rec.get("businessname1")).strip(), ein, str(rec.get("ein")).strip()})
        for col in NAME_LIST_COLS:
            for raw in _names(row.get(col)):
                if raw not in covered:
                    leftovers.append((pos, clean_funder_text(raw), "investor" if col == "Investors" else "donor"))

    ids_by_key, ids_by_pos_key, keys_by_pos = {}, {}, {}
    for pos, funder_id, name, *_ in out:
        key = funder_key(name)
        ids_by_key.setdefault(key, set()).add(funder_id)
        ids_by_pos_key.setdefault((pos, key), set()).add(funder_id)
        keys_by_pos.setdefault(pos, set()).add(key)
    for pos, name, source in leftovers:
        key = funder_key(name)
        if any(other.startswith(key + " ") for other in keys_by_pos.get(pos, ())):
            continue  # the cut-off first line of a funder this org already has
        ids = ids_by_pos_key.get((pos, key)) or ids_by_key.get(key, set())
        if len(ids) == 1:
            out.append((pos, next(iter(ids)), name, source, ""))
        elif slugify(name):
            out.append((pos, f"name:{slugify(name)}", name, source, ""))
    return pd.DataFrame(out, columns=["pos", "funder_id", "name", "source", "dba"]).drop_duplicates()


def funder_display_names(long, picks=None):
    """``funder_id`` -> its display name: the best spelling among its names, else the model's ``picks``."""
    names = long.groupby("funder_id")["name"].agg(pick_spelling)
    if picks is not None and len(picks):
        names.update(picks[picks.index.isin(names.index)])
    return names


GENERIC_WORDS = frozenset({"the", "of", "and", "for", "a", "inc", "corp", "foundation", "fund", "trust", "charitable"})


def _words(name):
    return set(re.findall(r"[a-z0-9]+", str(name).lower())) - GENERIC_WORDS


def _accepted(picks, rows):
    """``funder_id`` -> name for the answers of ``pick_display_names`` that can be used, and the ids that cannot.

    A ``low`` confidence answer is not used. Neither is a DBA chosen for a funder with no website and
    no word in common with its filed name: with no website to confirm it, the DBA is often another
    fund or program the filer manages ("JANE CAROL BRESSLER CHARITABLE FUND" with "H & N BLECHNER
    MEMORIAL FUND").
    """
    inputs = rows.set_index("funder_id")
    answers = picks.set_index("funder_id")
    unsupported_dba = [
        f for f in answers.index
        if answers.at[f, "source"] == "dba" and not isinstance(inputs.at[f, "website"], str)
        and not _words(inputs.at[f, "name"]) & _words(answers.at[f, "display_name"])
    ]
    usable = answers[(answers["confidence"] != "low") & (answers["display_name"].str.strip() != "")]
    usable = usable.drop(index=[f for f in unsupported_dba if f in usable.index])
    return usable["display_name"], set(inputs.index) - set(usable.index)


def pick_funder_names(names, dbas, websites, session, texts=None, limit=None):
    """``funder_id`` -> the name the model chose, for the funders it should look at.

    Giving Tuesday funders (``ein:``) with a DBA or a website go to the organization prompt, which
    chooses among the filed name, the DBA and the website, so it may replace the filed name; when
    other funders use the same website it names this one, not the network. An answer that is not
    used (see ``_accepted``) and the other names that are still ALL CAPS go to the filed-name prompt,
    which keeps every word of the filed name and fixes casing, a legal suffix and a cut-off; an answer
    that drops or replaces a word is not used.

    ``dbas`` and ``websites`` are ``funder_id`` -> DBA and website (missing or blank for none).
    ``texts`` is ``funder_id`` -> the start of the website's scraped text, shown to the model as for orgs;
    without it the model only sees the domain.
    ``limit`` sends only a random sample of that size from each group (siblings still come from all funders).
    """
    inputs = pd.DataFrame(
        {
            "funder_id": names.index,
            "name": names.values,
            "dba": names.index.map(dbas).fillna(""),
            "website": names.index.map(websites).map(website_domain),
        }
    )
    has_evidence = inputs["funder_id"].str.startswith("ein:") & ((inputs["dba"] != "") | inputs["website"].notna())
    evidence = inputs[has_evidence].copy()
    by_site = evidence.dropna(subset=["website"]).groupby("website")["name"].agg(list)
    evidence["siblings"] = [
        [n for n in by_site.get(site, []) if n != name] if isinstance(site, str) else []
        for site, name in zip(evidence["website"], evidence["name"])
    ]
    if limit:
        evidence = evidence.sample(min(limit, len(evidence)), random_state=0)
    shared = evidence["siblings"].map(len) > 0
    kwargs = dict(name_col="name", id_col="funder_id", dba_col="dba", website_col="website", max_errors=5)
    if texts is not None:
        evidence["text"] = evidence["funder_id"].map(texts)
        kwargs["text_col"] = "text"
    picked, rejected = [], set()
    for rows, extra in ((evidence[~shared], {"prompt": PROMPT}),
                        (evidence[shared], {"prompt": PROMPT_SHARED_SITE, "siblings_col": "siblings"})):
        if len(rows):
            accepted, refused = _accepted(pick_display_names(rows, session, **kwargs, **extra), rows)
            picked.append(accepted)
            rejected |= refused

    caps = inputs[(~has_evidence | inputs["funder_id"].isin(rejected)) & inputs["name"].map(lambda n: n.isupper() and len(n) > 4)]
    if limit:
        caps = caps.iloc[:limit]
    if len(caps):
        answer = pick_display_names(
            caps, session, name_col="name", id_col="funder_id", prompt=PROMPT_FILED_NAME, max_errors=5
        ).set_index("funder_id")["display_name"]
        filed = caps.set_index("funder_id")["name"].reindex(answer.index)
        kept = pd.Series(
            [funder_key(f) in funder_key(a) for f, a in zip(filed, answer)], index=answer.index, dtype=bool
        )
        logger.info("Funder names: %d filed-name answers removed or replaced a word and were not used", int((~kept).sum()))
        picked.append(answer[kept])
    logger.info(
        "Funder names: %d funders with a DBA or website (%d share a website), %d ALL CAPS without",
        len(evidence), int(shared.sum()), len(caps),
    )
    return pd.concat(picked) if picked else pd.Series(dtype=str)


def add_state_to_shared_names(names, states):
    """Append " (NC)" to the names that more than one funder id shares, so the player shows them apart.

    ``states`` is ``funder_id`` -> two-letter state (missing or ``None`` where unknown).
    Funders with a unique name, and shared names with no known state, keep their name.
    """
    shared = names[names.duplicated(keep=False)]
    if shared.empty:
        return names
    suffix = pd.Series(
        [f" ({s})" if isinstance(s, str) and s else "" for s in shared.index.map(states)], index=shared.index
    )
    names = names.copy()
    names.update(shared + suffix)
    logger.info(
        "Funder names: %d ids share a name with another id; %d still do after adding the state",
        len(shared), int(names[shared.index].duplicated(keep=False).sum()),
    )
    return names


def build_funder_columns(df, min_occurrences=2, session=None, attributes_of=None):
    """Add ``Funder_Ids`` and rebuild ``Investors``, ``Donors`` and ``Funders`` from funder ids.

    A funder linked to fewer than ``min_occurrences`` orgs is dropped; no org row is. Names are
    each funder's display name, so one funder has one spelling. ``attributes_of`` maps a list of funder
    ids to a frame indexed by id with the columns ``state`` and ``website``, and optionally ``text`` (the
    start of the website's scraped text). With it, funders that share a
    display name get their state added to it; with it and a database ``session``, the model also sees each
    Giving Tuesday funder's DBA and website (see ``pick_funder_names``). Without ``attributes_of`` the model
    only fixes the casing of the names that are still ALL CAPS.
    """
    long = collect_funders(df)
    names = funder_display_names(long)
    n_orgs = long.groupby("funder_id")["pos"].nunique()
    keep = n_orgs[n_orgs >= min_occurrences].index
    attributes = attributes_of(list(keep)) if attributes_of is not None else None
    if session is not None:
        dbas = long[long["dba"] != ""].groupby("funder_id")["dba"].agg(pick_spelling)
        websites = attributes["website"] if attributes is not None else pd.Series(dtype=object)
        texts = attributes["text"] if attributes is not None and "text" in attributes else None
        names = funder_display_names(long, pick_funder_names(names[keep], dbas, websites, session, texts))
    if attributes is not None:
        names.update(add_state_to_shared_names(names[keep], attributes["state"]))
    logger.info(
        "Funders: %d ids, %d linked to >= %d orgs; %d display names are shared by more than one id",
        len(n_orgs), len(keep), min_occurrences, int(names[names.index.isin(keep)].duplicated().sum()),
    )
    long = long[long["funder_id"].isin(keep)].assign(display=lambda d: d["funder_id"].map(names))
    names_by_pos = {}
    for source, col in (("investor", "Investors"), ("donor", "Donors")):
        names_by_pos[col] = long[long["source"] == source].groupby("pos")["display"].agg(lambda s: list(dict.fromkeys(s)))
    ordered = long.drop_duplicates(["pos", "funder_id"])
    ids_by_pos = ordered.groupby("pos")["funder_id"].agg(list)
    funders = ordered.groupby("pos")["display"].agg(list)
    positions = pd.RangeIndex(len(df))
    df = df.copy()
    df["Funder_Ids"] = [ids_by_pos.get(p, []) for p in positions]
    df["Funders"] = [funders.get(p, []) for p in positions]
    for col, series in names_by_pos.items():
        df[col] = [series.get(p, []) for p in positions]
    return df
