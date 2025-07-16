import os
import re
from pathlib import Path
from sqlalchemy import create_engine
import geopandas as gpd
import pandas as pd


class FCCTowerExtractor:
    def __init__(self, db_connection_url, organized_data_folder):
        """
        Initialize the FCC Tower Extractor

        Args:
            db_connection_url: Database connection string for PostgreSQL
            organized_data_folder: Path to your organized BEAD data folder
        """
        self.db_connection_url = db_connection_url
        self.organized_data_folder = Path(organized_data_folder)
        self.con = create_engine(db_connection_url)

        # State name to abbreviation mapping
        self.state_name_to_abbr = {
            'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
            'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
            'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
            'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
            'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
            'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
            'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
            'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
            'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
            'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
            'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
            'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
            'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia': 'DC'
        }

        # Structure type mapping
        self.structure_mapping = {
            "TA": "Tower Antenna", "GTA": "Guyed Tower", "LTA": "Lattice Tower",
            "MTA": "Monopole Tower", "TOWER": "Tower", "GTOWER": "Guyed Tower",
            "NTOWER": "Non-Guyed Tower", "MTOWER": "Monopole Tower", "LTOWER": "Lattice Tower",
            "POLE": "Utility Pole", "BPOLE": "Building-Mounted Pole", "3BPOLE": "Building-Mounted Pole",
            "BPIPE": "Building-Mounted Pipe", "BTWR": "Building Tower", "5BTWR": "Building Tower",
            "2BTWR": "Building Tower", "UPOLE": "Utility Pole", "PIPE": "Pipe Structure",
            "MAST": "Mast", "BMAST": "Building-Mounted Mast", "STACK": "Industrial Stack",
            "SIGN": "Sign Structure", "TREE": "Tree Structure", "RIG": "Oil or Gas Rig",
            "BRIDG": "Bridge Structure", "SILO": "Silo", "BANT": "Building Antenna",
            "3BANT": "Building Antenna", "NNTANN": "Non-Tower Antenna", "TANK": "Tank"
        }

    def extract_type(self, code):
        """Extract structure type from code and map to English description"""
        try:
            key = re.sub(r'[^A-Z]', '', str(code).upper())
            return self.structure_mapping.get(key, "Unknown")
        except (AttributeError, TypeError):
            return "Unknown"

    def get_state_folders(self):
        """Get all state folders from the organized data directory"""
        state_folders = []

        if not self.organized_data_folder.exists():
            print(f"Error: Organized data folder not found: {self.organized_data_folder}")
            return state_folders

        for item in self.organized_data_folder.iterdir():
            if item.is_dir() and item.name in self.state_name_to_abbr:
                state_folders.append(item.name)

        return sorted(state_folders)

    def extract_fcc_data_for_state(self, state_name):
        """Extract FCC tower data for a specific state"""

        # Get state abbreviation
        state_abbr = self.state_name_to_abbr.get(state_name)
        if not state_abbr:
            print(f"Error: Unknown state name '{state_name}'")
            return False

        print(f"Processing {state_name} ({state_abbr})...")

        try:
            # SQL query for the state
            query = f"""
            SELECT u.*,
                   c.county_name
            FROM us_antenna_structure_towers_test25_upd u
                     INNER JOIN
                 us_counties c
                 ON
                     LEFT(u.block_code, 5) = c.fips
            WHERE u.struc_state = '{state_abbr}'
              AND u.status_of_tower = 'Constructed';
            """

            # Execute query and create GeoDataFrame
            df = gpd.GeoDataFrame.from_postgis(query, self.con)

            if df.empty:
                print(f"  Warning: No tower data found for {state_name}")
                return False

            # Add English structure type mapping
            df["english_type"] = df["structure_type"].apply(self.extract_type)

            # Add tower ownership grouping
            df = self.add_tower_ownership_grouping(df)

            # Define output path
            state_folder = self.organized_data_folder / state_name
            output_file = state_folder / f"{state_name} FCC Antenna Structures.sqlite"

            # Save to SQLite
            df.to_file(output_file, driver='sqlite', encoding='utf-8')

            print(f"  ✓ Successfully saved {len(df):,} towers to: {output_file}")
            return True

        except Exception as e:
            print(f"  ✗ Error processing {state_name}: {str(e)}")
            return False

    def add_tower_ownership_grouping(self, df):
        """Add grouped_entity column based on tower ownership patterns"""

        def categorize_entity(entity_name):
            """Categorize entity into major tower companies or 'Other'"""
            if pd.isna(entity_name):
                return "Other"

            entity_lower = str(entity_name).lower()

            # American Tower patterns
            if any(pattern in entity_lower for pattern in [
                'american tower', 'amt', 'american towers'
            ]):
                return "American Towers"

            # SBA patterns
            elif any(pattern in entity_lower for pattern in [
                'sba', 'sba communications', 'sba comm'
            ]):
                return "SBA"

            # Crown Castle patterns
            elif any(pattern in entity_lower for pattern in [
                'crown castle', 'crown', 'ccic'
            ]):
                return "Crown Castle"

            # Everything else
            else:
                return "Other"

        # Apply categorization if 'entity' column exists
        if 'entity' in df.columns:
            df['grouped_entity'] = df['entity'].apply(categorize_entity)
        else:
            df['grouped_entity'] = 'Other'

        return df

    def extract_all_states(self, state_list=None):
        """Extract FCC tower data for all states or specified list"""

        if state_list is None:
            state_folders = self.get_state_folders()
        else:
            state_folders = state_list

        if not state_folders:
            print("No state folders found to process.")
            return

        print(f"Found {len(state_folders)} states to process:")
        print(f"States: {', '.join(state_folders)}")
        print("=" * 80)

        successful = 0
        failed = 0

        for state_name in state_folders:
            try:
                if self.extract_fcc_data_for_state(state_name):
                    successful += 1
                else:
                    failed += 1
            except KeyboardInterrupt:
                print("\n\nProcess interrupted by user.")
                break
            except Exception as e:
                print(f"  ✗ Unexpected error for {state_name}: {str(e)}")
                failed += 1

        # Summary
        print("=" * 80)
        print("EXTRACTION SUMMARY:")
        print(f"✓ Successful: {successful}")
        print(f"✗ Failed: {failed}")
        print(f"📁 Total processed: {successful + failed}")

        if failed > 0:
            print("\nNote: Failed extractions may be due to:")
            print("- No tower data in database for that state")
            print("- Database connection issues")
            print("- Missing permissions")

    def test_connection(self):
        """Test database connection"""
        try:
            # Simple test query
            test_query = "SELECT COUNT(*) as count FROM us_antenna_structure_towers_test25_upd LIMIT 1"
            result = pd.read_sql(test_query, self.con)
            print(f"✓ Database connection successful. Found {result['count'].iloc[0]:,} total towers.")
            return True
        except Exception as e:
            print(f"✗ Database connection failed: {str(e)}")
            return False


def main():
    # Configuration
    # UPDATE THESE PATHS AND CONNECTION STRING:
    DB_CONNECTION_URL = "postgresql://postgresqlwireless2020:software2020!!@wirelesspostgresqlflexible.postgres.database.azure.com:5432/wiroidb2"
    ORGANIZED_DATA_FOLDER = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data"
    # Initialize extractor
    extractor = FCCTowerExtractor(DB_CONNECTION_URL, ORGANIZED_DATA_FOLDER)

    # Test database connection first
    print("Testing database connection...")
    if not extractor.test_connection():
        print("Please check your database connection settings and try again.")
        return

    print("\n" + "=" * 80)
    print("FCC TOWER DATA EXTRACTION")
    print("=" * 80)

    # Option 1: Extract for all states
    # extractor.extract_all_states()

    # Option 2: Extract for specific states only (uncomment to use)
    specific_states = ['Delaware', 'Georgia', 'Hawaii', 'Louisiana', 'Minnesota', 'New Hampshire', 'New Jersey', 'Rhode Island', 'Vermont', 'Wyoming']
    extractor.extract_all_states(specific_states)

    print("\nFCC tower extraction completed!")





# Example usage for individual state:
def extract_single_state_example():
    """Example of how to extract data for a single state"""
    DB_CONNECTION_URL = "postgresql://postgresqlwireless2020:software2020!!@wirelesspostgresqlflexible.postgres.database.azure.com:5432/wiroidb2"
    ORGANIZED_DATA_FOLDER = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data"

    extractor = FCCTowerExtractor(DB_CONNECTION_URL, ORGANIZED_DATA_FOLDER)

    # Extract for just one state
    extractor.extract_fcc_data_for_state("Alaska")


if __name__ == "__main__":
    extract_single_state_example()
    # main()