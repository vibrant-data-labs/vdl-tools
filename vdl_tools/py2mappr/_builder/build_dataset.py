from collections import defaultdict
from pathlib import Path
import pandas as pd
from typing import Any, List, Dict, TypedDict

from vdl_tools.py2mappr._core.config import AttributeConfig, WeightedAttributeConfig, default_attr_config
from vdl_tools.py2mappr._builder._utils import md_to_html
import copy

from vdl_tools.py2mappr._db.geoquery import GeoItem, query_latlon


class Datapoint(TypedDict):
    id: str
    attr: Dict[str, Any]


class Dataset(TypedDict):
    attrDescriptors: List[AttributeConfig]
    datapoints: List[Datapoint]


def build_attr_descriptor(column: str, override: pd.Series) -> AttributeConfig:
    """
    Build an attribute descriptor from a column in a dataframe.

    Parameters
    ----------
    column : str. The column name in the dataframe.

    override : pd.Series. The series containing the override values for the
    attribute descriptor.

    Returns
    -------
    AttributeConfig. The attribute descriptor.
    """
    attrs: AttributeConfig = dict(copy.deepcopy(default_attr_config))

    # if title doesnt exist. copy from id.
    attrs["id"] = column
    attrs["title"] = attrs["id"] if attrs["title"] == "" else attrs["title"]

    # use if override exists
    if override is not None:
        for key, val in override.items():
            if key in attrs:
                attrs[key] = val

    return attrs


def build_attrDescriptors(
    data: Dict[str, AttributeConfig], attrs_df: pd.DataFrame = None
) -> List[AttributeConfig]:
    """
    Build the attribute descriptors for the dataset.

    Parameters
    ----------
    data : Dict[str, AttributeConfig]. The attribute descriptors for the
    dataset.

    attrs_df : pd.DataFrame, optional. The dataframe containing the attribute
    descriptors, by default None

    Returns
    -------
    List[AttributeConfig] The attribute descriptors for the dataset.
    """
    attrDescriptors = [
        build_attr_descriptor(key, attrs_df[key]) for key in data.keys()
    ]

    return attrDescriptors


def __build_datapoint(
    dp: pd.Series,
    dpAttribTypes: Dict[str, str],
    dpRenderTypes: Dict[str, str],
    exclude_md_attrs: List[str] = [],
    weighted_attributes: List[WeightedAttributeConfig] = [],
    geodata: dict[str, list] | None = None,
) -> Datapoint:
    attrs: Dict[str, Any] = dict(dp)

    # validate the attr vals based on type.
    for key, val in attrs.items():
        if dpAttribTypes[key] == "liststring":
            # check if value is string type
            if isinstance(val, str):
                # put string into a list
                attrs[key] = [val]
            elif not isinstance(val, list):
                attrs[key] = ""
        elif (
            dpAttribTypes[key] == "float"
            or dpAttribTypes[key] == "integer"
            or dpAttribTypes[key] == "year"
        ):
            attrs[key] = val if not pd.isna(val) else ""
        else:
            attrs[key] = val if type(val) is list or not pd.isna(val) else ""

        if (
            dpAttribTypes[key] == "string"
            and dpRenderTypes[key] == "text"
            and key not in exclude_md_attrs
        ):
            attrs[key] = md_to_html(attrs[key])

    # merge attrs with template
    datapoint_result = {
        "id": str(dp["id"]), 
        "attr": attrs,
        'geodata': geodata.get(str(dp["id"]), []) if geodata else []
    }

    if len(weighted_attributes) > 0:
        datapoint_result['weightedAttr'] = weighted_attributes
    
    return datapoint_result


def build_datapoints(
    df_datapoints: pd.DataFrame,
    dpAttribTypes: Dict[str, str],
    dpRenderTypes: Dict[str, str],
    exclude_md_attrs: List[str] = [],
    weighted_attributes: Dict[int, List[WeightedAttributeConfig]] = {},
    geodata_latlon: tuple[str, str] | None = None,
) -> List[Dict[str, Any]]:
    """
    Build the datapoints for the dataset.

    Parameters
    ----------
    df_datapoints : pd.DataFrame. The dataframe containing the datapoints.

    dpAttribTypes : Dict[str, str]. The attribute types for the datapoints.

    dpRenderTypes : Dict[str, str]. The render types for the datapoints.
    Returns
    -------
    List[Dict[str, Any]] The datapoints for the dataset.
    """

    geodata = defaultdict(list)
    if geodata_latlon is not None:
        lat, lon = geodata_latlon
        pts = [GeoItem(key=str(dp['id']), latitude=dp[lat], longitude=dp[lon])
               for _, dp in df_datapoints.iterrows()]
        for detail_level in range(0, 3):
            query_res = query_latlon(detail_level, pts)
            for node_item in query_res:
                for datapoint_id in node_item.points:
                    geodata[datapoint_id].append({
                        'level': detail_level,
                        'polygon_id': node_item.osm_id
                    })

    datapoints = [
        __build_datapoint(dp, dpAttribTypes, dpRenderTypes, exclude_md_attrs,
                          weighted_attributes[dp["id"]] if dp["id"] in weighted_attributes else [],
                          geodata)
        for _, dp in df_datapoints.iterrows()
    ]

    return datapoints
