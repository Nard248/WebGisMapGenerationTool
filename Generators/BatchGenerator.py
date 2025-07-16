import os
import time
from pathlib import Path
from FCCTower import create_map_fcc_towers  # Import your mapping function


class BatchMapGenerator:
    def __init__(self, data_folder, nationwide_cbrs_file):
        """
        Initialize the batch map generator

        Args:
            data_folder: Path to your organized BEAD data folder
            nationwide_cbrs_file: Path to the nationwide CBRS Excel file
        """
        self.data_folder = Path(data_folder)
        self.nationwide_cbrs_file = nationwide_cbrs_file

    def get_state_folders(self):
        """Get all state folders from the data directory"""
        state_folders = []

        if not self.data_folder.exists():
            print(f"Error: Data folder not found: {self.data_folder}")
            return state_folders

        for item in self.data_folder.iterdir():
            if item.is_dir():
                # Check if it's a valid state folder by looking for required files
                state_outline = item / f"{item.name} State Outline.sqlite"
                county_outline = item / f"{item.name} County Outline.sqlite"
                bead_eligible = item / f"{item.name} BEAD Eligible Locations.sqlite"
                grid_analysis = item / f"{item.name} BEAD Grid Analysis Layer.sqlite"

                if all(file.exists() for file in [state_outline, county_outline, bead_eligible, grid_analysis]):
                    state_folders.append(item.name)
                else:
                    print(f"⚠️  Skipping {item.name} - missing required files")

        return sorted(state_folders)

    def find_antenna_file(self, state_folder):
        """Find the antenna/tower file for a state"""
        state_path = self.data_folder / state_folder

        # Look for the standardized antenna file
        antenna_file = state_path / f"{state_folder} FCC Antenna Structures.sqlite"
        if antenna_file.exists():
            return str(antenna_file)

        # Look for other variations
        variations = [
            f"{state_folder} Antenna Structures.sqlite",
            f"{state_folder} Towers.sqlite",
            f"{state_folder} FCC Towers.sqlite"
        ]

        for variation in variations:
            file_path = state_path / variation
            if file_path.exists():
                return str(file_path)

        # Look for any .sqlite file containing "antenna", "tower", or "fcc"
        for file in state_path.glob("*.sqlite"):
            filename_lower = file.name.lower()
            if any(keyword in filename_lower for keyword in ['antenna', 'tower', 'fcc']):
                return str(file)

        return None

    def find_additional_bead_files(self, state_folder):
        """Find Round 2 and CAI files for a state if they exist"""
        state_path = self.data_folder / state_folder

        # Look for Round 2 file
        round2_file = state_path / f"{state_folder} BEAD Eligible Locations Round 2.sqlite"
        round2_path = str(round2_file) if round2_file.exists() else None

        # Look for CAI file
        cai_file = state_path / f"{state_folder} BEAD Eligible CAIs.sqlite"
        cai_path = str(cai_file) if cai_file.exists() else None

        return round2_path, cai_path

    def find_cci_files(self, state_folder):
        """Find CCI DS and CCI Fiber files for Maine if they exist"""
        state_path = self.data_folder / state_folder

        # Only look for CCI files if this is Maine
        if state_folder.lower() != 'maine':
            return None, None

        # Look for CCI DS file
        cci_ds_file = state_path / "CCI DSL.sqlite"
        cci_ds_path = str(cci_ds_file) if cci_ds_file.exists() else None

        # Look for CCI Fiber file
        cci_fiber_file = state_path / "CCI Fiber.sqlite"
        cci_fiber_path = str(cci_fiber_file) if cci_fiber_file.exists() else None

        return cci_ds_path, cci_fiber_path

    def generate_map_for_state(self, state_name):
        """Generate map for a specific state"""
        print(f"🗺️  Processing {state_name}...")

        try:
            # Build paths
            base_folder = str(self.data_folder / state_name)
            antenna_file = self.find_antenna_file(state_name)
            round2_file, cai_file = self.find_additional_bead_files(state_name)
            cci_ds_file, cci_fiber_file = self.find_cci_files(state_name)

            # Check if antenna file exists
            if antenna_file:
                print(f"    📡 Found antenna file: {Path(antenna_file).name}")
            else:
                print(f"    ⚠️  No antenna file found for {state_name}")

            # Check for additional BEAD files
            additional_files = []
            if round2_file:
                additional_files.append(f"Round 2: {Path(round2_file).name}")
            if cai_file:
                additional_files.append(f"CAI: {Path(cai_file).name}")

            if additional_files:
                print(f"    📋 Additional BEAD files found: {', '.join(additional_files)}")

            # Check for Maine-specific CCI files
            if state_name.lower() == 'maine':
                cci_files = []
                if cci_ds_file:
                    cci_files.append(f"CCI DSL: {Path(cci_ds_file).name}")
                if cci_fiber_file:
                    cci_files.append(f"CCI Fiber: {Path(cci_fiber_file).name}")

                if cci_files:
                    print(f"    📶 Maine CCI files found: {', '.join(cci_files)}")

            # Call the mapping function
            start_time = time.time()

            create_map_fcc_towers(
                base_folder=base_folder,
                state_name=state_name,
                antenna_file=antenna_file if antenna_file else '',
                cbrs_file=self.nationwide_cbrs_file,
                round2_locations_file=round2_file,
                cai_locations_file=cai_file,
                cci_ds_file=cci_ds_file,
                cci_fiber_file=cci_fiber_file
            )

            elapsed_time = time.time() - start_time
            print(f"    ✅ Map completed in {elapsed_time:.1f} seconds")
            return True

        except Exception as e:
            print(f"    ❌ Error generating map for {state_name}: {str(e)}")
            return False

    def generate_all_maps(self, state_list=None):
        """Generate maps for all states or specified list"""

        if state_list is None:
            state_folders = self.get_state_folders()
        else:
            state_folders = state_list

        if not state_folders:
            print("No valid state folders found to process.")
            return

        print("🚀 BATCH MAP GENERATION STARTED")
        print("=" * 80)
        print(f"📁 Data folder: {self.data_folder}")
        print(f"📊 CBRS file: {self.nationwide_cbrs_file}")
        print(f"🗺️  States to process: {len(state_folders)}")
        print(f"📋 States: {', '.join(state_folders)}")
        print("=" * 80)

        successful = 0
        failed = 0
        start_time = time.time()

        for i, state_name in enumerate(state_folders, 1):
            print(f"\n[{i}/{len(state_folders)}] {state_name}")

            try:
                if self.generate_map_for_state(state_name):
                    successful += 1
                else:
                    failed += 1
            except KeyboardInterrupt:
                print("\n\n⏹️  Process interrupted by user.")
                break
            except Exception as e:
                print(f"    ❌ Unexpected error for {state_name}: {str(e)}")
                failed += 1

        # Summary
        total_time = time.time() - start_time
        print("\n" + "=" * 80)
        print("📊 BATCH GENERATION SUMMARY")
        print("=" * 80)
        print(f"✅ Successfully generated: {successful} maps")
        print(f"❌ Failed: {failed} maps")
        print(f"📁 Total processed: {successful + failed} states")
        print(f"⏱️  Total time: {total_time / 60:.1f} minutes")
        print(f"⚡ Average time per map: {total_time / (successful + failed):.1f} seconds")

        if successful > 0:
            print(f"\n🎉 Maps saved to each state's Results folder!")
            print(f"📂 Example: {self.data_folder}/Alabama/Results/Alabama BEAD Map with FCC Towers.html")


def main():
    # Configuration - UPDATE THESE PATHS:
    DATA_FOLDER = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data"
    NATIONWIDE_CBRS_FILE = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data\CBRSCounties.xlsx"  # UPDATE THIS PATH

    # Verify files exist
    if not os.path.exists(DATA_FOLDER):
        print(f"❌ Error: Data folder not found: {DATA_FOLDER}")
        return

    if not os.path.exists(NATIONWIDE_CBRS_FILE):
        print(f"❌ Error: CBRS file not found: {NATIONWIDE_CBRS_FILE}")
        print("Please update the NATIONWIDE_CBRS_FILE path in the script.")
        return

    # Initialize generator
    generator = BatchMapGenerator(DATA_FOLDER, NATIONWIDE_CBRS_FILE)

    # Option 1: Generate for all states
    # generator.generate_all_maps()

    # Option 2: Generate for specific states only (uncomment to use)
    # specific_states = ['Washington', 'New Hampshire', 'Vermont', 'Illinois', 'Texas', 'Oklahoma', 'Utah', 'California', 'Colorado', 'Washington', 'Delaware', 'Georgia', 'Hawaii', 'Louisiana', 'Minnesota', 'New Hampshire', 'New Jersey', 'Rhode Island', 'Vermont', 'Wyoming', 'Maryland']
    # specific_states = ['Texas', 'Oklahoma', 'Utah', 'California', 'Colorado', 'Washington']
    specific_states = ['Delaware', 'Georgia', 'Hawaii', 'Louisiana', 'Minnesota', 'New Hampshire', 'New Jersey', 'Rhode Island', 'Vermont', 'Wyoming', 'Maryland']

    generator.generate_all_maps(specific_states)


def test_single_state():
    """Test function for generating a single state map"""
    DATA_FOLDER = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data"
    NATIONWIDE_CBRS_FILE = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data\CBRSCounties.xlsx"

    generator = BatchMapGenerator(DATA_FOLDER, NATIONWIDE_CBRS_FILE)

    # Test with a single state
    generator.generate_map_for_state("Maine")


def test_maine_state():
    """Test function for generating Maine map with CCI layers"""
    DATA_FOLDER = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data"
    NATIONWIDE_CBRS_FILE = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data\CBRSCounties.xlsx"

    generator = BatchMapGenerator(DATA_FOLDER, NATIONWIDE_CBRS_FILE)

    # Test with Maine to check CCI layers
    generator.generate_map_for_state("Maine")


def preview_states():
    """Preview which states will be processed"""
    DATA_FOLDER = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data"
    NATIONWIDE_CBRS_FILE = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data\CBRSCounties.xlsx"

    generator = BatchMapGenerator(DATA_FOLDER, NATIONWIDE_CBRS_FILE)
    states = generator.get_state_folders()

    print("🔍 PREVIEW: States that will be processed:")
    print("=" * 50)
    for i, state in enumerate(states, 1):
        # Check for antenna file
        antenna_file = generator.find_antenna_file(state)
        antenna_status = "📡" if antenna_file else "⚠️ "

        # Check for additional BEAD files
        round2_file, cai_file = generator.find_additional_bead_files(state)
        additional_status = ""
        if round2_file or cai_file:
            extras = []
            if round2_file:
                extras.append("R2")
            if cai_file:
                extras.append("CAI")
            additional_status = f" (+{'/'.join(extras)})"

        # Check for Maine CCI files
        cci_status = ""
        if state.lower() == 'maine':
            cci_ds_file, cci_fiber_file = generator.find_cci_files(state)
            if cci_ds_file or cci_fiber_file:
                cci_extras = []
                if cci_ds_file:
                    cci_extras.append("CCI DSL")
                if cci_fiber_file:
                    cci_extras.append("CCI Fiber")
                cci_status = f" (+{'/'.join(cci_extras)})"

        print(f"{i:2d}. {antenna_status} {state}{additional_status}{cci_status}")

    print(f"\n📊 Total: {len(states)} states ready for processing")

    # Show summary
    with_antenna = sum(1 for state in states if generator.find_antenna_file(state))
    without_antenna = len(states) - with_antenna

    with_additional = sum(1 for state in states if any(generator.find_additional_bead_files(state)))

    # Check for Maine CCI files
    maine_with_cci = 0
    if 'Maine' in states:
        cci_ds_file, cci_fiber_file = generator.find_cci_files('Maine')
        if cci_ds_file or cci_fiber_file:
            maine_with_cci = 1

    print(f"📡 With antenna files: {with_antenna}")
    print(f"⚠️  Without antenna files: {without_antenna}")
    print(f"📋 With additional BEAD data (Round 2/CAI): {with_additional}")
    if maine_with_cci:
        print(f"📶 Maine with CCI layers: {maine_with_cci}")


if __name__ == "__main__":
    # Uncomment the function you want to run:

    # Preview what will be processed
    # preview_states()

    # Generate maps for all states
    main()

    # Test with a single state first
    # test_single_state()

    # Test Maine specifically with CCI layers
    # test_maine_state()

# Colorado Washington Round 1 - 2
# Texas Oklahoma Utah California