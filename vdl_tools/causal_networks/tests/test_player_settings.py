"""The attribute settings table -> py2mappr update_attributes arguments."""
import pandas as pd

from vdl_tools.causal_networks import player


def test_default_table_has_every_setting_column_and_no_priority():
    table = player.DEFAULT_ATTRIBUTE_SETTINGS
    assert set(player.SETTING_COLUMNS) <= set(table.columns)
    assert "low_priority" not in table.columns
    assert (table["Keep"] == 1).all()
    rename, drop, kwargs = player.read_attribute_settings(table)
    assert rename == {} and drop == []
    assert "Catalytic Score" in kwargs["visible_filters"]
    assert "Causal Cluster" in kwargs["horizontal_bars"]
    assert kwargs["attr_descriptions"]["Trophic Level"].startswith("Causal flow position")
    assert "id" not in kwargs["visible_profile"]


def test_table_rename_keep_and_missing_columns(tmp_path):
    table = pd.DataFrame([
        dict(Attribute="Keystone Rank", Display_Name="Leverage Rank", Keep=1, visible_filters=1, axis_select=1,
             tooltip="renamed"),
        dict(Attribute="Betweenness", Keep=0),
        dict(Attribute="Pillar", visible_profile=1, tags_1=1),   # no Keep / Display_Name columns filled
    ])
    path = tmp_path / "settings.xlsx"
    player.write_attribute_settings(path, table)
    rename, drop, kwargs = player.read_attribute_settings(path)
    assert rename == {"Keystone Rank": "Leverage Rank"}
    assert drop == ["Betweenness"]
    assert kwargs["visible_filters"] == ["Leverage Rank"]
    assert kwargs["axis_select"] == ["Leverage Rank"]
    assert kwargs["wide_tags"] == ["Pillar"]           # tags_1 -> wide_tags
    assert kwargs["visible_profile"] == ["Pillar"]
    assert kwargs["attr_descriptions"] == {"Leverage Rank": "renamed"}
    assert kwargs["visible_search"] == []              # column absent from the table -> nothing selected
