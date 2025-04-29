from typing import Any, List, Literal, TypedDict

from vdl_tools.py2mappr._attributes.attr_types import ATTR_TYPE, RENDER_TYPE, VISIBILITY_AREAS


class SponsorInfo(TypedDict):
    """
    The SponsorInfo class is a TypedDict that contains the information for a
    sponsor of the project.
    """

    iconUrl: str
    linkUrl: str
    linkTitle: str


class FeedbackInfo(TypedDict):
    """
    The FeedbackInfo class is a TypedDict that contains the information for
    feedback of the project.
    """

    type: Literal["email", "link"]
    link: str
    text: str


class ProjectConfig(TypedDict):
    """
    The ProjectConfig class is a TypedDict that contains the configuration for
    the project.
    """

    headerTitle: str
    modalDescription: str
    modalSubtitle: str
    displayTooltipCard: bool
    startPage: Literal["filter", "legend", "list"]
    showStartInfo: bool
    defaultPanel: str
    sponsors: List[SponsorInfo]
    projectLogoTitle: str
    projectLogoUrl: str
    displayExportButton: bool
    beta: bool
    sponsorsTxt: str
    feedback: FeedbackInfo
    socials: List[Literal["twitter", "linkedin", "facebook"]]
    footer: Any


base_config: ProjectConfig = {
    "headerTitle": "map title",
    "modalDescription": "<p>Note: <i>This visualization is designed for desktop viewing and has not been optimized for mobile. It works best in Chrome or Safari.</i></p>",
    "modalSubtitle": "<p>map subtitle</p>",
    "displayTooltipCard": False,
    "startPage": "legend",
    "showStartInfo": True,
    "defaultPanel": "Map Information",
    "sponsors": [],
    "projectLogoTitle": "openmappr | network exploration tool",
    "projectLogoUrl": None,
    "sharingLogoUrl": None,
    "displayExportButton": False,
    "beta": False,
    "sponsorsTxt": "Sponsored by",
    "feedback": {"type": "email", "link": "mailto:", "text": "Feedback"},
    "footer": None,
    "socials": [],
}


class AttributeConfig(TypedDict):
    """
    The AttributeConfig class is a TypedDict that contains the configuration for
    an attribute to be used in the project.
    """

    id: str
    title: str
    attrType: ATTR_TYPE
    renderType: RENDER_TYPE
    visibility: VISIBILITY_AREAS
    priority: Literal["high", "medium", "low"]
    axis: Literal["x", "y", "none"]
    tooltip: str
    colorSelectable: bool
    sizeSelectable: bool


class WeightedAttributeConfig(TypedDict):
    id: str
    values: List[str]
    weights: List[float]


class NetworkAttributeConfig(TypedDict):
    """
    The NetworkAttributeConfig class is a TypedDict that contains the configuration for
    an edge attribute to be used in the project.
    """

    id: str
    title: str
    visible: bool
    searchable: bool
    attrType: ATTR_TYPE
    renderType: RENDER_TYPE
    visibleInProfile: bool


default_attr_config: AttributeConfig = {
    "id": "",
    "title": "",
    "visibility": [],
    "attrType": "string",
    "renderType": "default",
    "priority": "medium",
    "axis": "none",
    "tooltip": "",
    "colorSelectable": False,
    "sizeSelectable": False,
}

default_net_attr_config: NetworkAttributeConfig = {
    "id": "OriginalLabel",
    "title": "OriginalLabel",
    "visible": False,
    "searchable": False,
    "attrType": "liststring",
    "renderType": "tag-cloud",
    "visibleInProfile": False,
}
