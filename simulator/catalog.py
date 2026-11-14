"""Static game catalog: fictional champions, items and teams (no real game IP)."""

from __future__ import annotations

CHAMPIONS: tuple[str, ...] = (
    "ch_emberwyn", "ch_thornveil", "ch_quillon", "ch_morrowgrim", "ch_vexalith",
    "ch_solbrand", "ch_nyxara", "ch_gravelock", "ch_lumivane", "ch_kestrova",
    "ch_ashkarn", "ch_brinewhisper", "ch_cindrel", "ch_duskmantle", "ch_frostgale",
    "ch_hollowmere", "ch_ironpetal", "ch_jadecoil", "ch_ravensong", "ch_zephyrin",
)

# (item_id, gold cost)
ITEMS: tuple[tuple[str, int], ...] = (
    ("it_iron_sigil", 400), ("it_ember_charm", 850), ("it_warden_plate", 1100),
    ("it_stormglass", 1300), ("it_veil_cloak", 900), ("it_runic_blade", 1450),
    ("it_swift_greaves", 300), ("it_moonwell_flask", 150),
)

TEAMS: tuple[str, str] = ("blue", "red")
