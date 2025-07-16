import os
import math
import random
import folium
from folium.plugins import MarkerCluster
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union


def load_cbrs_data(cbrs_file):
    """Loads CBRS license data into a dictionary indexed by county name."""
    cbrs_df = pd.read_excel(cbrs_file, usecols=["Channel", "county_name", "bidder", "state_abbr"])
    return cbrs_df.groupby("county_name") \
        .apply(lambda x: x.to_dict(orient="records")) \
        .to_dict()


def load_cbrs_data_filtered(cbrs_file, state_abbr):
    """Loads CBRS license data filtered by state and indexed by county name."""
    if not cbrs_file or not os.path.exists(cbrs_file):
        return {}

    cbrs_df = pd.read_excel(cbrs_file, usecols=["Channel", "county_name", "bidder", "state_abbr"])

    # Filter by state abbreviation first
    state_filtered_df = cbrs_df[cbrs_df['state_abbr'] == state_abbr]

    if state_filtered_df.empty:
        return {}

    return state_filtered_df.groupby("county_name") \
        .apply(lambda x: x.to_dict(orient="records")) \
        .to_dict()


# State name to abbreviation mapping
STATE_NAME_TO_ABBR = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
    # Territories
    'District of Columbia': 'DC',
    'Puerto Rico': 'PR',
    'Virgin Islands': 'VI',
    'American Samoa': 'AS',
    'Guam': 'GU',
    'Northern Mariana Islands': 'MP'
}


# Replace the CBRS data loading section in your function with:
def get_state_abbreviation(state_name):
    """Get state abbreviation from state name, with fallback handling."""
    # Direct lookup
    if state_name in STATE_NAME_TO_ABBR:
        return STATE_NAME_TO_ABBR[state_name]

    # Handle title case variations
    title_case_name = state_name.title()
    if title_case_name in STATE_NAME_TO_ABBR:
        return STATE_NAME_TO_ABBR[title_case_name]

    # Handle some common variations
    variations = {
        'District Of Columbia': 'DC',
        'Washington DC': 'DC',
        'Washington D.C.': 'DC'
    }

    if state_name in variations:
        return variations[state_name]

    # If no match found, print warning and return None
    print(f"Warning: Could not find state abbreviation for '{state_name}'")
    return None


def get_wisp_color(wisp_name):
    """
    Returns a color from a known dictionary or picks a random color if there's no match.
    """
    wisp_colors = {
        "AT&T": "#009FDB",
        "T-Mobile": "#E20074",
        "Verizon": "#E81123",
        "Mediacom Bolt": "#0033A0",
        "Point Broadband": "#F89728",
        "Cloud 9 Wireless": "#6BACE4",
        "Dragonfly Internet": "#FF5733",
        "Rapid Wireless LLC": "#28A745",
        "Wildstar Networks": "#8E44AD"
    }
    for key in wisp_colors:
        if key.lower() in wisp_name.lower().strip():
            return wisp_colors[key]
    return "#{:06x}".format(random.randint(0, 0xFFFFFF))


def add_wisp_layers(m, wisp_folder):
    """
    Reads each .sqlite WISP file, picks a color, and adds it as a separate layer.
    """
    if not os.path.isdir(wisp_folder):
        return m

    for file in os.listdir(wisp_folder):
        if file.endswith(".sqlite"):
            file_path = os.path.join(wisp_folder, file)
            wisp_layer = gpd.read_file(file_path)[['geometry']].set_crs("EPSG:4326").to_crs("EPSG:4326")
            wisp_name = os.path.splitext(file)[0]
            color = get_wisp_color(wisp_name)

            wisp_fg = folium.FeatureGroup(name=f"WISP - {wisp_name}", show=False)
            folium.GeoJson(
                wisp_layer,
                style_function=lambda x, c=color: {
                    "fillColor": c,
                    "color": c,
                    "weight": 1,
                    "fillOpacity": 0.6,
                },
            ).add_to(wisp_fg)
            wisp_fg.add_to(m)

    return m


def create_cluster_icon_function():
    """Create the JavaScript function for cluster color coding"""
    return """
    function(cluster) {
        var childCount = cluster.getChildCount();
        var ranges = [
            {range: [1, 50], color: "#00ff00"},
            {range: [50, 100], color: "#80ff00"},
            {range: [100, 200], color: "#ffff00"},
            {range: [200, 400], color: "#ffbf00"},
            {range: [400, 700], color: "#ff8000"},
            {range: [700, 1000], color: "#ff4000"},
            {range: [1000, 2000], color: "#ff0000"},
            {range: [2000, Infinity], color: "#800000"}
        ];
        var rangeMatch = ranges.find(r => childCount >= r.range[0] && childCount <= r.range[1]);
        var clusterColor = rangeMatch ? rangeMatch.color : "#333333";

        function getTextColor(hexColor) {
            var r = parseInt(hexColor.substring(1, 3), 16);
            var g = parseInt(hexColor.substring(3, 5), 16);
            var b = parseInt(hexColor.substring(5, 7), 16);
            var brightness = (r*299 + g*587 + b*114)/1000;
            return (brightness > 125) ? "black" : "white";
        }
        var textColor = getTextColor(clusterColor);

        return new L.DivIcon({
            html: `
                <div style="
                    display:flex; align-items:center; justify-content:center;
                    width:40px; height:40px;
                    border-radius:50%;
                    background: radial-gradient(circle, ${clusterColor} 60%, rgba(255,255,255,0) 100%);
                    color:${textColor};
                    font-weight:bold;
                ">
                    ${childCount}
                </div>
            `,
            className: 'marker-cluster'
        });
    }
    """


def add_bead_locations_layer(m, locations_data, layer_name, color, border_color, show=True):
    """Add a BEAD locations layer with clustering"""
    if locations_data is None or locations_data.empty:
        return

    bead_feature_group = folium.FeatureGroup(name=layer_name, show=show)

    # Create marker cluster with the shared icon function
    cluster_colors_js = create_cluster_icon_function()
    marker_cluster = MarkerCluster(
        disableClusteringAtZoom=11,
        icon_create_function=cluster_colors_js
    ).add_to(bead_feature_group)

    # Add markers to cluster
    for _, row in locations_data.iterrows():
        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=4,
            color=border_color,
            fill=True,
            fill_color=color,
            fill_opacity=1,
            weight=1,
        ).add_to(marker_cluster)

    bead_feature_group.add_to(m)


def add_cci_layers(m, cci_ds_file, cci_fiber_file):
    """Add CCI DSL and CCI Fiber layers for Maine"""

    # Add CCI DSL layer
    if cci_ds_file and os.path.exists(cci_ds_file):
        try:
            cci_ds_data = gpd.read_file(cci_ds_file).set_crs("EPSG:4326").to_crs("EPSG:4326")
            cci_ds_fg = folium.FeatureGroup(name="CCI DSL", show=False)

            folium.GeoJson(
                cci_ds_data,
                style_function=lambda x: {
                    "fillColor": "#FF6B35",  # Orange color for DS
                    "color": "#FF6B35",
                    "weight": 2,
                    "fillOpacity": 0.7,
                },
            ).add_to(cci_ds_fg)
            cci_ds_fg.add_to(m)
            print(f"    📶 Added CCI DSL layer: {len(cci_ds_data)} features")
        except Exception as e:
            print(f"    ⚠️  Error loading CCI DSL file: {e}")

    # Add CCI Fiber layer
    if cci_fiber_file and os.path.exists(cci_fiber_file):
        try:
            cci_fiber_data = gpd.read_file(cci_fiber_file).set_crs("EPSG:4326").to_crs("EPSG:4326")
            cci_fiber_fg = folium.FeatureGroup(name="CCI Fiber", show=False)

            folium.GeoJson(
                cci_fiber_data,
                style_function=lambda x: {
                    "fillColor": "#4ECDC4",  # Teal color for Fiber
                    "color": "#4ECDC4",
                    "weight": 2,
                    "fillOpacity": 0.7,
                },
            ).add_to(cci_fiber_fg)
            cci_fiber_fg.add_to(m)
            print(f"    🌐 Added CCI Fiber layer: {len(cci_fiber_data)} features")
        except Exception as e:
            print(f"    ⚠️  Error loading CCI Fiber file: {e}")


def create_map_fcc_towers(
        base_folder,
        state_name,
        antenna_file='',
        wisp_folder='',
        state_outline_file='',
        county_outline_file='',
        bead_eligible_locations_file='',
        grid_analysis_layer_file='',
        output_path='',
        cbrs_file='',
        round2_locations_file=None,
        cai_locations_file=None,
        cci_ds_file=None,
        cci_fiber_file=None,
):
    """
    Creates a Folium map with:
     1. State/County Outlines + CBRS popups
     2. WISP coverage
     3. BEAD Eligible marker clusters (Round 1, Round 2, CAI when available)
     4. CCI DSL and CCI Fiber layers (Maine specific)
     5. Grid Analysis
     6. Antenna Markers & Coverage Circles hidden until zoom >= antenna_zoom_threshold
    """
    # --- 1) Resolve file paths ---
    if not wisp_folder:
        if os.path.exists(os.path.join(base_folder, f"{state_name} WISPs Hex Dissolved")):
            wisp_folder = os.path.join(base_folder, f"{state_name} WISPs Hex Dissolved")
        elif os.path.exists(os.path.join(base_folder, f"{state_name} WISPs Dissolved")):
            wisp_folder = os.path.join(base_folder, f"{state_name} WISPs Dissolved")
    if not state_outline_file:
        state_outline_file = os.path.join(base_folder, f"{state_name} State Outline.sqlite")
    if not county_outline_file:
        county_outline_file = os.path.join(base_folder, f"{state_name} County Outline.sqlite")
    if not bead_eligible_locations_file:
        bead_eligible_locations_file = os.path.join(base_folder, f"{state_name} BEAD Eligible Locations.sqlite")
    if not grid_analysis_layer_file:
        grid_analysis_layer_file = os.path.join(base_folder, f"{state_name} BEAD Grid Analysis Layer.sqlite")
    if not output_path:
        output_path = os.path.join(base_folder, "Results")

    # --- 2) Load data ---
    state_outline = gpd.read_file(state_outline_file)[['geometry']].set_crs("EPSG:4326").to_crs("EPSG:4326")
    county_outline = gpd.read_file(county_outline_file).set_crs("EPSG:4326").to_crs("EPSG:4326")

    # Load CBRS data
    state_abbr = get_state_abbreviation(state_name)
    if state_abbr and cbrs_file and os.path.exists(cbrs_file):
        cbrs_data = load_cbrs_data_filtered(cbrs_file, state_abbr)
    else:
        cbrs_data = {}
        if cbrs_file:
            print(f"Warning: CBRS file not found or invalid state name: {cbrs_file}")

    # Load BEAD data
    bead_eligible_locations = gpd.read_file(bead_eligible_locations_file).set_crs("EPSG:4326").to_crs("EPSG:4326")

    # Load Round 2 data if available
    bead_eligible_locations_round_2 = None
    if round2_locations_file and os.path.exists(round2_locations_file):
        bead_eligible_locations_round_2 = gpd.read_file(round2_locations_file).set_crs("EPSG:4326").to_crs("EPSG:4326")
        print(f"    📋 Loaded Round 2 locations: {len(bead_eligible_locations_round_2)} points")

    # Load CAI data if available
    bead_eligible_cai = None
    if cai_locations_file and os.path.exists(cai_locations_file):
        if state_abbr == 'CA':
            bead_eligible_cai = gpd.read_file(cai_locations_file, columns='geometry').set_crs("EPSG:4326").to_crs(
                "EPSG:4326")
            print(f"    📋 Loaded CAI locations: {len(bead_eligible_cai)} points for California")
        else:
            bead_eligible_cai = gpd.read_file(cai_locations_file).set_crs("EPSG:4326").to_crs("EPSG:4326")
            print(f"    📋 Loaded CAI locations: {len(bead_eligible_cai)} points")
    grid_analysis_layer = gpd.read_file(grid_analysis_layer_file)[['geometry', 'point_count']].set_crs("EPSG:4326").to_crs("EPSG:4326")

    range_dict = {
        "0": {"range": (1, 5), "color": "#e4e4f3"},
        "1": {"range": (5, 10), "color": "#d1d1ea"},
        "2": {"range": (10, 20), "color": "#b3b3e0"},
        "3": {"range": (20, 30), "color": "#8080c5"},
        "4": {"range": (30, 50), "color": "#6d6dbd"},
        "5": {"range": (50, 75), "color": "#4949ac"},
        "6": {"range": (75, 100), "color": "#3737a4"},
        "7": {"range": (100, 50000), "color": "#121293"},
    }

    # --- 3) Create Folium map centered on state centroid ---
    centroid = state_outline.geometry.centroid.iloc[0]
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=7, tiles=None)

    # --- 4) Base layers (White BG, Google Maps, Satellite) ---
    folium.TileLayer("", attr="White Background", name="White Background").add_to(m)
    folium.TileLayer(
        tiles="http://www.google.cn/maps/vt?lyrs=m&x={x}&y={y}&z={z}",
        attr="Google Maps",
        name="Google Maps",
        control=True
    ).add_to(m)
    folium.TileLayer(
        tiles="http://www.google.cn/maps/vt?lyrs=s&x={x}&y={y}&z={z}",
        attr="Google Satellite",
        name="Google Satellite"
    ).add_to(m)

    css_white_bg = """
    <style>
    .leaflet-container {
        background-color: white;
    }
    </style>
    """
    m.get_root().html.add_child(folium.Element(css_white_bg))

    js_white_bg_toggle = """
    <script>
    document.addEventListener("DOMContentLoaded", function () {
        function toggleWhiteBackground(active) {
            var mapContainer = document.querySelector('.leaflet-container');
            if (active) {
                mapContainer.style.backgroundColor = 'white';
            } else {
                mapContainer.style.backgroundColor = '';
            }
        }
        var layerControl = document.querySelector('.leaflet-control-layers-list');
        if (layerControl) {
            layerControl.addEventListener('input', function () {
                var whiteBackgroundInput = document.querySelector("input[name='White Background']");
                if (whiteBackgroundInput) {
                    toggleWhiteBackground(whiteBackgroundInput.checked);
                }
            });
        }
    });
    </script>
    """
    m.get_root().html.add_child(folium.Element(js_white_bg_toggle))

    # --- 5) State Outline ---
    fg_state = folium.FeatureGroup(name="State Outline")
    folium.GeoJson(
        state_outline,
        style_function=lambda x: {
            "fillColor": "none",
            "color": "red",
            "weight": 2,
            "fillOpacity": 0,
        },
    ).add_to(fg_state)
    fg_state.add_to(m)

    # --- 6) County Outline + CBRS table popups ---
    fg_county = folium.FeatureGroup(name="County Outline")
    folium.GeoJson(
        county_outline,
        style_function=lambda x: {
            "fillColor": "none",
            "color": "blue",
            "weight": 1,
            "fillOpacity": 0,
        },
    ).add_to(fg_county)

    for _, row in county_outline.iterrows():
        county_name = row.get("name", "Unknown County")
        cbrs_info = cbrs_data.get(county_name, [])
        # Build the CBRS table
        popup_html = """
        <style>
        .cbrs-table {
            width: 100%;
            border-collapse: collapse;
            font-family: Arial, sans-serif;
        }
        .cbrs-table th, .cbrs-table td {
            border: 1px solid #ddd;
            padding: 15px;
            text-align: left;
            min-width: 150px;
        }
        .cbrs-table th {
            background-color: #4CAF50;
            color: white;
            font-weight: bold;
        }
        .cbrs-table tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        .cbrs-table tr:hover {
            background-color: #ddd;
        }
        </style>
        <b>CBRS PAL License Holders</b>
        <table class='cbrs-table'>
          <tr><th>Channel</th><th>County</th><th>Bidder</th></tr>
        """
        for entry in cbrs_info:
            popup_html += (
                f"<tr>"
                f"<td>{entry['Channel']}</td>"
                f"<td>{entry['county_name']}</td>"
                f"<td>{entry['bidder']}</td>"
                f"</tr>"
            )
        popup_html += "</table>"

        # Fixed marker - improved clickability and removed background
        folium.Marker(
            location=[row.geometry.centroid.y, row.geometry.centroid.x],
            icon=folium.DivIcon(
                html=(
                    f'<div style="'
                    f'font-size:14px; '
                    f'font-weight:bold; '
                    f'text-shadow: -1px -1px 0 white, 1px -1px 0 white, -1px 1px 0 white, 1px 1px 0 white; '
                    f'width:auto; '
                    f'text-align:center; '
                    f'pointer-events:auto; '  # Ensure clickability
                    f'padding:0; '
                    f'white-space:nowrap; '  # Prevent text wrapping
                    f'">{county_name}</div>',
                ),
                icon_size=(0, 0),  # Remove the default icon size constraint
                class_name='county-label'  # Give it a class name for potential CSS targeting
            ),
            popup=folium.Popup(popup_html, max_width=500)
        ).add_to(fg_county)
    fg_county.add_to(m)

    # --- 7) BEAD Eligible Locations (Multiple layers) ---
    # Determine layer naming and visibility based on what data is available
    has_round2 = bead_eligible_locations_round_2 is not None
    has_cai = bead_eligible_cai is not None

    if has_round2 or has_cai:
        # If we have additional data, show Round 1 as hidden and name it specifically
        round1_name = 'BEAD Eligible Locations Round 1'
        round1_show = True
    else:
        # If we only have Round 1 data, show it by default with simple name
        round1_name = 'BEAD Eligible Locations'
        round1_show = True

    # Add Round 1 locations
    add_bead_locations_layer(m, bead_eligible_locations, round1_name, "#01fbff", "white", round1_show)

    # Add Round 2 locations if available
    if has_round2:
        add_bead_locations_layer(m, bead_eligible_locations_round_2, 'BEAD Eligible Locations Round 2', "#01fbff",
                                 "black", False)

    # Add CAI locations if available
    if has_cai:
        add_bead_locations_layer(m, bead_eligible_cai, 'BEAD Eligible CAIs', "#01fbff", "black", False)

    # --- 8) Add CCI Layers (Maine specific) ---
    if state_name.lower() == 'maine' and (cci_ds_file or cci_fiber_file):
        add_cci_layers(m, cci_ds_file, cci_fiber_file)

    # --- 9) Grid Analysis layers (dissolved polygons) ---
    for key, val in range_dict.items():
        rng_min, rng_max = val["range"]
        color = val["color"]
        subset = grid_analysis_layer[
            (grid_analysis_layer["point_count"] >= rng_min) &
            (grid_analysis_layer["point_count"] < rng_max)
            ]
        if len(subset) == 0:
            continue
        from shapely.ops import unary_union
        dissolved_geo = unary_union(subset["geometry"])
        dissolved_gdf = gpd.GeoDataFrame(geometry=[dissolved_geo], crs="EPSG:4326")

        if rng_min == 100:
            layer_name = f"Grid Layer ({rng_min}+ Locations)"
        else:
            layer_name = f"Grid Layer ({rng_min}-{rng_max} Locations)"

        fg_grid = folium.FeatureGroup(name=layer_name, show=False)
        folium.GeoJson(
            dissolved_gdf,
            style_function=lambda x, c=color: {
                "fillColor": c,
                "color": c,
                "weight": 1,
                "fillOpacity": 0.6,
            },
        ).add_to(fg_grid)
        fg_grid.add_to(m)

    # --- 10) Add WISP Layers ---
    add_wisp_layers(m, wisp_folder)

    # --- 11) Antenna Markers + Coverage => hide below zoom ---
    if antenna_file and os.path.exists(antenna_file):
        antenna_data = gpd.read_file(antenna_file, encoding='utf-8').set_crs("EPSG:4326").to_crs("EPSG:4326")
        american_tower_data = antenna_data[antenna_data['grouped_entity'] == "American Towers"]
        sba_tower_data = antenna_data[antenna_data['grouped_entity'] == "SBA"]
        crown_castle_tower_data = antenna_data[antenna_data['grouped_entity'] == "Crown Castle"]
        other_tower_data = antenna_data[antenna_data['grouped_entity'] == "Other"]
        print(other_tower_data.head())
        print(american_tower_data.head())
        print(sba_tower_data.head())
        print(crown_castle_tower_data.head())
        fg_antennas = folium.FeatureGroup(name="Antenna Locations - Other", show=False)
        fg_antennas_american_towers = folium.FeatureGroup(name="Antenna Locations - American Towers", show=False)
        fg_antennas_sba_towers = folium.FeatureGroup(name="Antenna Locations - SBA Towers", show=False)
        fg_antennas_crown_castle = folium.FeatureGroup(name="Antenna Locations - Crown Castle Towers", show=False)
        fg_coverage2 = folium.FeatureGroup(name="Antenna Coverage (2 Miles)", show=False)
        fg_coverage5 = folium.FeatureGroup(name="Antenna Coverage (5 Miles)", show=False)

        def degrees_per_mile(lat, miles):
            return miles / (69.0 * math.cos(math.radians(lat)))

        coverage_styles = {
            "Other": {"color": "blue"},
            "American Towers": {"color": "red"},
            "SBA": {"color": "purple"},
            "Crown Castle": {"color": "orange"},
        }

        # Helper function for styled ring overlays
        def add_coverage_rings(group_data, group_name, group_color, fg_2mi, fg_5mi):
            for _, row in group_data.iterrows():
                lat, lon = row.geometry.y, row.geometry.x
                r2 = degrees_per_mile(lat, 2) * 111000
                r5 = degrees_per_mile(lat, 5) * 111000

                folium.Circle(
                    location=[lat, lon],
                    radius=r2,
                    color=group_color,
                    weight=2,
                    fill=True,
                    fillColor=group_color,
                    fillOpacity=0.2,
                    dash_array=None,
                ).add_to(fg_2mi)

                folium.Circle(
                    location=[lat, lon],
                    radius=r5,
                    color=group_color,
                    weight=2,
                    fill=True,
                    fillColor=group_color,
                    fillOpacity=0.1,
                    dash_array="10,5"
                ).add_to(fg_5mi)

        # Create separate feature groups for coverage by tower owner
        fg_coverage2_other = folium.FeatureGroup(name="Other - Coverage 2 Mile", show=False)
        fg_coverage5_other = folium.FeatureGroup(name="Other - Coverage 5 Mile", show=False)
        fg_coverage2_american = folium.FeatureGroup(name="American Towers - Coverage 2 Mile", show=False)
        fg_coverage5_american = folium.FeatureGroup(name="American Towers - Coverage 5 Mile", show=False)
        fg_coverage2_sba = folium.FeatureGroup(name="SBA Towers - Coverage 2 Mile", show=False)
        fg_coverage5_sba = folium.FeatureGroup(name="SBA Towers - Coverage 5 Mile", show=False)
        fg_coverage2_crown = folium.FeatureGroup(name="Crown Castle - Coverage 2 Mile", show=False)
        fg_coverage5_crown = folium.FeatureGroup(name="Crown Castle - Coverage 5 Mile", show=False)

        # Apply coverage per group
        add_coverage_rings(other_tower_data, "Other", coverage_styles["Other"]["color"], fg_coverage2_other,
                           fg_coverage5_other)
        add_coverage_rings(american_tower_data, "American Towers", coverage_styles["American Towers"]["color"],
                           fg_coverage2_american, fg_coverage5_american)
        add_coverage_rings(sba_tower_data, "SBA", coverage_styles["SBA"]["color"], fg_coverage2_sba, fg_coverage5_sba)
        add_coverage_rings(crown_castle_tower_data, "Crown Castle", coverage_styles["Crown Castle"]["color"],
                           fg_coverage2_crown, fg_coverage5_crown)

        # Add coverage layers to map

        # Add the global copy function to the map once
        copy_function_js = """
        <script>
        window.copyTableContent = function(button) {
            // Find the table in the same popup
            var table = button.parentElement.querySelector('.tower-table');
            var rows = table.querySelectorAll('tr');
            var textContent = 'FCC Tower Information:\\n\\n';

            // Convert table to text format suitable for email
            for (var i = 0; i < rows.length; i++) {
                var cells = rows[i].querySelectorAll('td');
                if (cells.length === 2) {
                    var label = cells[0].textContent.trim();
                    var value = cells[1].textContent.trim();
                    textContent += label + ': ' + value + '\\n';
                }
            }

            // Copy to clipboard
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(textContent).then(function() {
                    // Visual feedback
                    var originalText = button.textContent;
                    button.textContent = 'Copied!';
                    button.style.backgroundColor = '#45a049';
                    setTimeout(function() {
                        button.textContent = originalText;
                        button.style.backgroundColor = '#4CAF50';
                    }, 1000);
                }).catch(function(err) {
                    fallbackCopy(textContent, button);
                });
            } else {
                fallbackCopy(textContent, button);
            }
        };

        function fallbackCopy(text, button) {
            var textArea = document.createElement('textarea');
            textArea.value = text;
            textArea.style.position = 'fixed';
            textArea.style.opacity = '0';
            document.body.appendChild(textArea);
            textArea.select();
            try {
                document.execCommand('copy');
                var originalText = button.textContent;
                button.textContent = 'Copied!';
                setTimeout(function() {
                    button.textContent = originalText;
                }, 1000);
            } catch (err) {
                console.error('Copy failed:', err);
            }
            document.body.removeChild(textArea);
        }
        </script>
        """
        m.get_root().html.add_child(folium.Element(copy_function_js))

        for idx, row in antenna_data.iterrows():
            lat, lon = row.geometry.y, row.geometry.x

            r2 = degrees_per_mile(lat, 2) * 111000
            r5 = degrees_per_mile(lat, 5) * 111000

            # Circles - remove className that might be hiding them
            folium.Circle(
                location=[lat, lon],
                radius=r2,
                color="green",
                fill=True,
                fillColor="green",
                fillOpacity=0.2
            ).add_to(fg_coverage2)

            folium.Circle(
                location=[lat, lon],
                radius=r5,
                color="blue",
                fill=True,
                fillColor="blue",
                fillOpacity=0.1
            ).add_to(fg_coverage5)

        for idx, row in other_tower_data.iterrows():

            lat, lon = row.geometry.y, row.geometry.x
            tower_popup_html = f"""
            <style>
            .popup-container-{idx} {{
                position: relative;
                width: 100%;
            }}
            .copy-btn-{idx} {{
                position: absolute;
                top: 5px;
                right: 5px;
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 5px 10px;
                border-radius: 3px;
                cursor: pointer;
                font-size: 12px;
                z-index: 1000;
            }}
            .copy-btn-{idx}:hover {{
                background-color: #45a049;
            }}
            .copy-btn-{idx}:active {{
                background-color: #3d8b40;
            }}
            .tower-table-{idx} {{
                width: 100%;
                border-collapse: collapse;
                font-family: Arial, sans-serif;
                margin-top: 25px;
            }}
            .tower-table-{idx} th, .tower-table-{idx} td {{
                border: 1px solid #ddd;
                padding: 10px;
                text-align: left;
                min-width: 120px;
            }}
            .tower-table-{idx} th {{
                background-color: #2196F3;
                color: white;
                font-weight: bold;
            }}
            .tower-table-{idx} tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
            .tower-table-{idx} tr:hover {{
                background-color: #ddd;
            }}
            </style>

            <div class="popup-container-{idx}">
                <button class="copy-btn-{idx}" onclick="window.copyTableContent(this)">Copy</button>
                <b>FCC Tower Information</b>
                <table class='tower-table tower-table-{idx}'>
            """

            tower_columns = {
                "Latitude": row.get("lat", "N/A"),
                "Longitude": row.get("lon", "N/A"),
                "County Name": f"{row.get('county_name', 'N/A')} ({row.get('state_fips', 'N/A')}{row.get('county_fips', 'N/A')})",
                "Overall Height Above Ground (Meters)": row.get("overall_height_above_ground", "N/A"),
                "Type": f"{row.get('english_type', 'N/A')} ({row.get('structure_type', 'N/A')})",
                "Owner": row.get('entity', 'N/A')
            }

            for label, value in tower_columns.items():
                tower_popup_html += f"<tr><td><b>{label}</b></td><td>{value}</td></tr>"

            tower_popup_html += """
                </table>
            </div>
            """

            # Marker icon
            folium.Marker(
                location=[lat, lon],
                icon=folium.Icon(
                    icon="wifi",
                    prefix="fa",
                    color="blue",
                    icon_color="white"
                ),
                popup=folium.Popup(tower_popup_html, max_width=400)
            ).add_to(fg_antennas)
        for idx, row in american_tower_data.iterrows():

            lat, lon = row.geometry.y, row.geometry.x
            tower_popup_html = f"""
            <style>
            .popup-container-{idx} {{
                position: relative;
                width: 100%;
            }}
            .copy-btn-{idx} {{
                position: absolute;
                top: 5px;
                right: 5px;
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 5px 10px;
                border-radius: 3px;
                cursor: pointer;
                font-size: 12px;
                z-index: 1000;
            }}
            .copy-btn-{idx}:hover {{
                background-color: #45a049;
            }}
            .copy-btn-{idx}:active {{
                background-color: #3d8b40;
            }}
            .tower-table-{idx} {{
                width: 100%;
                border-collapse: collapse;
                font-family: Arial, sans-serif;
                margin-top: 25px;
            }}
            .tower-table-{idx} th, .tower-table-{idx} td {{
                border: 1px solid #ddd;
                padding: 10px;
                text-align: left;
                min-width: 120px;
            }}
            .tower-table-{idx} th {{
                background-color: #2196F3;
                color: white;
                font-weight: bold;
            }}
            .tower-table-{idx} tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
            .tower-table-{idx} tr:hover {{
                background-color: #ddd;
            }}
            </style>

            <div class="popup-container-{idx}">
                <button class="copy-btn-{idx}" onclick="window.copyTableContent(this)">Copy</button>
                <b>FCC Tower Information</b>
                <table class='tower-table tower-table-{idx}'>
            """

            tower_columns = {
                "Latitude": row.get("lat", "N/A"),
                "Longitude": row.get("lon", "N/A"),
                "County Name": f"{row.get('county_name', 'N/A')} ({row.get('state_fips', 'N/A')}{row.get('county_fips', 'N/A')})",
                "Overall Height Above Ground (Meters)": row.get("overall_height_above_ground", "N/A"),
                "Type": f"{row.get('english_type', 'N/A')} ({row.get('structure_type', 'N/A')})",
                "Owner": row.get('entity', 'N/A')
            }

            for label, value in tower_columns.items():
                tower_popup_html += f"<tr><td><b>{label}</b></td><td>{value}</td></tr>"

            tower_popup_html += """
                </table>
            </div>
            """

            # Marker icon
            folium.Marker(
                location=[lat, lon],
                icon=folium.Icon(
                    icon="wifi",
                    prefix="fa",
                    color="red",
                    icon_color="white"
                ),
                popup=folium.Popup(tower_popup_html, max_width=400)
            ).add_to(fg_antennas_american_towers)
        for idx, row in sba_tower_data.iterrows():

            lat, lon = row.geometry.y, row.geometry.x
            tower_popup_html = f"""
                <style>
                .popup-container-{idx} {{
                    position: relative;
                    width: 100%;
                }}
                .copy-btn-{idx} {{
                    position: absolute;
                    top: 5px;
                    right: 5px;
                    background-color: #4CAF50;
                    color: white;
                    border: none;
                    padding: 5px 10px;
                    border-radius: 3px;
                    cursor: pointer;
                    font-size: 12px;
                    z-index: 1000;
                }}
                .copy-btn-{idx}:hover {{
                    background-color: #45a049;
                }}
                .copy-btn-{idx}:active {{
                    background-color: #3d8b40;
                }}
                .tower-table-{idx} {{
                    width: 100%;
                    border-collapse: collapse;
                    font-family: Arial, sans-serif;
                    margin-top: 25px;
                }}
                .tower-table-{idx} th, .tower-table-{idx} td {{
                    border: 1px solid #ddd;
                    padding: 10px;
                    text-align: left;
                    min-width: 120px;
                }}
                .tower-table-{idx} th {{
                    background-color: #2196F3;
                    color: white;
                    font-weight: bold;
                }}
                .tower-table-{idx} tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                .tower-table-{idx} tr:hover {{
                    background-color: #ddd;
                }}
                </style>

                <div class="popup-container-{idx}">
                    <button class="copy-btn-{idx}" onclick="window.copyTableContent(this)">Copy</button>
                    <b>FCC Tower Information</b>
                    <table class='tower-table tower-table-{idx}'>
                """

            tower_columns = {
                "Latitude": row.get("lat", "N/A"),
                "Longitude": row.get("lon", "N/A"),
                "County Name": f"{row.get('county_name', 'N/A')} ({row.get('state_fips', 'N/A')}{row.get('county_fips', 'N/A')})",
                "Overall Height Above Ground (Meters)": row.get("overall_height_above_ground", "N/A"),
                "Type": f"{row.get('english_type', 'N/A')} ({row.get('structure_type', 'N/A')})",
                "Owner": row.get('entity', 'N/A')
            }

            for label, value in tower_columns.items():
                tower_popup_html += f"<tr><td><b>{label}</b></td><td>{value}</td></tr>"

            tower_popup_html += """
                    </table>
                </div>
                """

            # Marker icon
            folium.Marker(
                location=[lat, lon],
                icon=folium.Icon(
                    icon="wifi",
                    prefix="fa",
                    color="purple",
                    icon_color="white"
                ),
                popup=folium.Popup(tower_popup_html, max_width=400)
            ).add_to(fg_antennas_sba_towers)
        for idx, row in crown_castle_tower_data.iterrows():

            lat, lon = row.geometry.y, row.geometry.x
            tower_popup_html = f"""
                <style>
                .popup-container-{idx} {{
                    position: relative;
                    width: 100%;
                }}
                .copy-btn-{idx} {{
                    position: absolute;
                    top: 5px;
                    right: 5px;
                    background-color: #4CAF50;
                    color: white;
                    border: none;
                    padding: 5px 10px;
                    border-radius: 3px;
                    cursor: pointer;
                    font-size: 12px;
                    z-index: 1000;
                }}
                .copy-btn-{idx}:hover {{
                    background-color: #45a049;
                }}
                .copy-btn-{idx}:active {{
                    background-color: #3d8b40;
                }}
                .tower-table-{idx} {{
                    width: 100%;
                    border-collapse: collapse;
                    font-family: Arial, sans-serif;
                    margin-top: 25px;
                }}
                .tower-table-{idx} th, .tower-table-{idx} td {{
                    border: 1px solid #ddd;
                    padding: 10px;
                    text-align: left;
                    min-width: 120px;
                }}
                .tower-table-{idx} th {{
                    background-color: #2196F3;
                    color: white;
                    font-weight: bold;
                }}
                .tower-table-{idx} tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                .tower-table-{idx} tr:hover {{
                    background-color: #ddd;
                }}
                </style>

                <div class="popup-container-{idx}">
                    <button class="copy-btn-{idx}" onclick="window.copyTableContent(this)">Copy</button>
                    <b>FCC Tower Information</b>
                    <table class='tower-table tower-table-{idx}'>
                """

            tower_columns = {
                "Latitude": row.get("lat", "N/A"),
                "Longitude": row.get("lon", "N/A"),
                "County Name": f"{row.get('county_name', 'N/A')} ({row.get('state_fips', 'N/A')}{row.get('county_fips', 'N/A')})",
                "Overall Height Above Ground (Meters)": row.get("overall_height_above_ground", "N/A"),
                "Type": f"{row.get('english_type', 'N/A')} ({row.get('structure_type', 'N/A')})",
                "Owner": row.get('entity', 'N/A')
            }

            for label, value in tower_columns.items():
                tower_popup_html += f"<tr><td><b>{label}</b></td><td>{value}</td></tr>"

            tower_popup_html += """
                    </table>
                </div>
                """

            # Marker icon
            folium.Marker(
                location=[lat, lon],
                icon=folium.Icon(
                    icon="wifi",
                    prefix="fa",
                    color="orange",
                    icon_color="white"
                ),
                popup=folium.Popup(tower_popup_html, max_width=400)
            ).add_to(fg_antennas_crown_castle)

    # Add these layers to the map
    fg_antennas.add_to(m)
    fg_coverage2_other.add_to(m)
    fg_coverage5_other.add_to(m)

    fg_antennas_american_towers.add_to(m)
    fg_coverage2_american.add_to(m)
    fg_coverage5_american.add_to(m)

    fg_antennas_sba_towers.add_to(m)
    fg_coverage2_sba.add_to(m)
    fg_coverage5_sba.add_to(m)

    fg_antennas_crown_castle.add_to(m)
    fg_coverage2_crown.add_to(m)
    fg_coverage5_crown.add_to(m)

    # --- 12) LayerControl etc. ---
    folium.LayerControl(collapsed=True).add_to(m)

    # Fix layer control toggling
    js_layer_control_fix = """
        <script>
        document.addEventListener("DOMContentLoaded", function () {
            var layerControl = document.querySelector('.leaflet-control-layers');
            if (layerControl) {
                var toggleBtn = document.querySelector('.leaflet-control-layers-toggle');
                if (!toggleBtn) {
                    toggleBtn = document.createElement('div');
                    toggleBtn.className = 'leaflet-control-layers-toggle';
                    layerControl.appendChild(toggleBtn);
                }
                toggleBtn.addEventListener('click', function () {
                    layerControl.classList.toggle('leaflet-control-layers-expanded');
                });
            }
        });
        </script>
        """
    m.get_root().html.add_child(folium.Element(js_layer_control_fix))

    # Remove default "Leaflet" attribution if you like
    js_remove_attribution = """
        <script>
        document.addEventListener("DOMContentLoaded", function() {
            var attrDiv = document.querySelector(".leaflet-bottom.leaflet-right");
            if (attrDiv) attrDiv.remove();
        });
        </script>
        """
    m.get_root().html.add_child(folium.Element(js_remove_attribution))

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    out_file = os.path.join(output_path, f"{state_name} BEAD Map with FCC Towers.html")
    print('saved map in:', out_file)
    m.save(out_file)

    return m