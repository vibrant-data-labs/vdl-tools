from copy import deepcopy
import plotly.graph_objects as go
import plotly.io as pio

from vdl_tools.py2mappr.vdl_palette import cat_palette, one_earth_colors

# default colors
VDL_COLORS = [x['col'] for x in cat_palette]
one_earth_colors = [x['col'] for x in one_earth_colors]


vibrant_white = deepcopy(pio.templates['simple_white'])
vibrant_white.layout.colorway = VDL_COLORS

pio.templates['vibrant_white'] = vibrant_white
pio.templates.default = "vibrant_white"


one_earth_white = deepcopy(pio.templates['simple_white'])
one_earth_white.layout.colorway = one_earth_colors
pio.templates['one_earth_white'] = one_earth_white