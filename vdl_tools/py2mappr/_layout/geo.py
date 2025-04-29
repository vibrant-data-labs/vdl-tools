from ._layout import Layout, LayoutSettings
from ._settings import base_layout_settings
from .._attributes import utils as attrutils
import copy
from typing import Literal

geo_base_settings: LayoutSettings = {
    **copy.deepcopy(base_layout_settings),
    **LayoutSettings(
        # valid for geo only
        mapboxMapID="mapbox/light-v10",
        drawGroupLabels=False,
        isGeo=True,
    ),
}

ZOOM_LEVELS = Literal['nodes', 'counties', 'states', 'countries']

class GeoConfig:
    min_level: ZOOM_LEVELS
    max_level: ZOOM_LEVELS
    default_level: ZOOM_LEVELS
    def __init__(self,
                 min_level: ZOOM_LEVELS = 'nodes',
                 max_level: ZOOM_LEVELS = 'countries',
                 default_level: ZOOM_LEVELS = 'countries'):
        self.min_level = min_level
        self.max_level = max_level
        self.default_level = default_level

def geo_level_mapping(zoom_level: ZOOM_LEVELS):
    return {
        'nodes': 'node',
        'counties': 'adm_districts',
        'states': 'fed_districts',
        'countries': 'countries'
    }[zoom_level]

class GeoLayout(Layout):
    """
    Describes the attributes for the geo layout. "plotType": "geo"
    """

    def __init__(
        self,
        project,
        settings=copy.deepcopy(geo_base_settings),
        x_axis="Latitude",
        y_axis="Longitude",
        name=None,
        descr=None,
        subtitle=None,
        image=None,
        geo_config: GeoConfig = GeoConfig()
    ):
        super().__init__(
            settings, "geo", x_axis, y_axis, name, descr, subtitle, image
        )
        self.geo_config = geo_config
        self.calculate_layout(project)

    def calculate_layout(self, project):
        self.x_axis, self.y_axis = attrutils.find_node_xy_attr(
            project.dataFrame, "lat", "long"
        )
        self.settings["nodeImageAttr"] = attrutils.find_node_image_attr(
            project.dataFrame
        )
        self.settings["nodePopImageAttr"] = attrutils.find_node_image_attr(
            project.dataFrame
        )
        self.settings["labelAttr"] = attrutils.find_node_label_attr(
            project.dataFrame
        )
        self.settings["labelHoverAttr"] = attrutils.find_node_label_attr(
            project.dataFrame
        )
        self.settings["nodeColorAttr"] = attrutils.find_node_color_attr(
            project.dataFrame
        )
        self.settings["nodeSizeAttr"] = attrutils.find_node_size_attr(
            project.dataFrame
        )

        if project.network is not None:
            self.settings["edgeColorAttr"] = attrutils.find_node_color_attr(
                project.network
            )
            self.settings["edgeSizeAttr"] = attrutils.find_node_size_attr(
                project.network, ["source", "target"]
            )


    def set_geo_config(self,
                 min_level: ZOOM_LEVELS = 'nodes',
                 max_level: ZOOM_LEVELS = 'countries',
                 default_level: ZOOM_LEVELS = 'countries'):
        self.geo_config = GeoConfig(min_level, max_level, default_level)


    def toDict(self):
        return {
            **super().toDict(),
            "geo": {
                "minLevel": geo_level_mapping(self.geo_config.min_level),
                "maxLevel": geo_level_mapping(self.geo_config.max_level),
                "defaultLevel": geo_level_mapping(self.geo_config.default_level)
            },
            "layout": {
                "plotType": "geo",
                "xaxis": self.x_axis,
                "yaxis": self.y_axis,
                "settings": self.settings,
            },
        }
