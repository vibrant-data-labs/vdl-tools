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

from vdl_tools.scrape_enrich.url_keys import website_domain
from vdl_tools.scrape_enrich.display_name import pick_names
from vdl_tools.shared_tools.funders.ids import normalize_ein, slugify
from vdl_tools.shared_tools.funders.names import (
    clean_funder_text,
    funder_key,
    funder_name_and_dba,
    pick_spelling,
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


def _unsupported_dba(answers, rows):
    """True for a DBA chosen for a funder with no website and no word in common with its filed name.

    With no website to confirm it, the DBA is often another fund or program the filer manages
    ("JANE CAROL BRESSLER CHARITABLE FUND" with "H & N BLECHNER MEMORIAL FUND").
    """
    inputs = rows.set_index("funder_id")
    return pd.Series(
        [
            source == "dba" and not isinstance(inputs.at[f, "website"], str)
            and not _words(inputs.at[f, "name"]) & _words(answer)
            for f, source, answer in zip(answers["funder_id"], answers["source"], answers["display_name"])
        ],
        index=answers.index,
    )


def pick_funder_names(names, dbas, websites, session, texts=None, domain_cache=None):
    """``funder_id`` -> the name the model chose, for the funders it should look at.

    Giving Tuesday funders (``ein:``) with a DBA or a website go through ``pick_names`` with the choice
    of name, so the model may replace the filed name with the DBA or the website's name; a DBA it
    cannot confirm is not used (``_unsupported_dba``). The other names that are still ALL CAPS only get
    their casing fixed, keeping every word.

    ``dbas`` and ``websites`` are ``funder_id`` -> DBA and website (missing or blank for none).
    ``texts`` is ``funder_id`` -> the start of the website's scraped text; without it the model only
    sees the domain. ``domain_cache`` (``website_quality.load_domain_cache``) groups funders whose
    websites redirect to one site.
    """
    inputs = pd.DataFrame(
        {
            "funder_id": names.index,
            "name": names.values,
            "dba": names.index.map(dbas).fillna(""),
            "website": names.index.map(websites).map(website_domain).map(lambda d: d or None),
        }
    )
    has_evidence = inputs["funder_id"].str.startswith("ein:") & ((inputs["dba"] != "") | inputs["website"].notna())
    kwargs = dict(dba_col="dba", website_col="website", max_errors=5)
    if texts is not None:
        inputs["text"] = inputs["funder_id"].map(texts)
        kwargs["text_col"] = "text"
    chosen = pick_names(
        inputs[has_evidence], session, "funder_id", "name", domain_cache=domain_cache, refuse=_unsupported_dba, **kwargs
    )
    caps = inputs[~has_evidence & inputs["name"].map(lambda n: n.isupper() and len(n) > 4)]
    cased = pick_names(caps, session, "funder_id", "name", choose=False, max_errors=5)
    logger.info(
        "Funder names: %d funders with a DBA or website, %d ALL CAPS without", int(has_evidence.sum()), len(caps)
    )
    return pd.concat([chosen, cased])


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


def funder_names(df, min_occurrences=2, session=None, attributes_of=None, domain_cache=None):
    """The names of the funders linked to at least ``min_occurrences`` orgs, indexed by ``funder_id``.

    ``display_name`` is chosen once for every place that shows the funder (the player, the dashboard):
    the best spelling among its names, else the model's choice. ``label`` is what the player shows: the
    display name with the state added where several funders share it ("Smith Family Foundation (NC)").

    ``attributes_of`` maps a list of funder ids to a frame indexed by id with the columns ``state`` and
    ``website``, and optionally ``text`` (the start of the website's scraped text). With it, funders that
    share a display name get their state in the label; with it and a database ``session``, the model also
    sees each Giving Tuesday funder's DBA and website (see ``pick_funder_names``, which ``domain_cache``
    goes to). Without ``attributes_of`` the model only fixes the casing of the names that are still ALL CAPS.
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
        names = funder_display_names(long, pick_funder_names(names[keep], dbas, websites, session, texts, domain_cache))
    names = names[keep]
    labels = add_state_to_shared_names(names, attributes["state"]) if attributes is not None else names
    logger.info(
        "Funders: %d ids, %d linked to >= %d orgs; %d labels are shared by more than one id",
        len(n_orgs), len(keep), min_occurrences, int(labels.duplicated().sum()),
    )
    return pd.DataFrame({"display_name": names, "label": labels}).rename_axis("funder_id")


def build_funder_columns(df, labels):
    """Add ``Funder_Ids`` and rebuild ``Investors``, ``Donors`` and ``Funders`` from funder ids.

    ``labels`` is ``funder_id`` -> the name to show (``funder_names(df)["label"]``), so one funder has
    one spelling. A funder not in it is dropped; no org row is.
    """
    long = collect_funders(df)
    long = long[long["funder_id"].isin(labels.index)].assign(display=lambda d: d["funder_id"].map(labels))
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
