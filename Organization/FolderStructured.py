import os
import shutil
import pandas as pd
from pathlib import Path
import re
from collections import defaultdict


class BEADFileOrganizer:
    def __init__(self, folder_report_csv, target_base_folder):
        """
        Initialize the organizer with the folder report CSV and target directory

        Args:
            folder_report_csv: Path to the CSV file containing all file paths
            target_base_folder: Base directory where organized folders will be created
        """
        self.folder_report_csv = folder_report_csv
        self.target_base_folder = Path(target_base_folder)
        self.file_groups = self._load_and_categorize_files()

    def _load_and_categorize_files(self):
        """Load the folder report and categorize files by type and state"""
        df = pd.read_csv(self.folder_report_csv)

        file_groups = {
            'state_outlines': {},
            'county_outlines': {},
            'bead_eligible': {},
            'grid_analysis': {},
            'wisps_hex': {},
            'wisps_regular': {},
            'antenna_files': {},
            'cbrs_files': {},
            'other': []
        }

        for _, row in df.iterrows():
            full_path = row['FullName']
            if not os.path.exists(full_path):
                continue

            filename = os.path.basename(full_path)

            # Extract state name and categorize
            state_name = self._extract_state_name(filename, full_path)
            if state_name == 'SKIP':
                continue

            if 'State Outline' in filename and filename.endswith('.sqlite'):
                file_groups['state_outlines'][state_name] = full_path
            elif 'County Outline' in filename and filename.endswith('.sqlite'):
                file_groups['county_outlines'][state_name] = full_path
            elif 'BEAD Eligible Locations' in filename and filename.endswith('.sqlite'):
                if state_name not in file_groups['bead_eligible']:
                    file_groups['bead_eligible'][state_name] = []
                file_groups['bead_eligible'][state_name].append(full_path)
            elif 'BEAD Grid Analysis Layer' in filename and filename.endswith('.sqlite'):
                if state_name not in file_groups['grid_analysis']:
                    file_groups['grid_analysis'][state_name] = []
                file_groups['grid_analysis'][state_name].append(full_path)
            elif 'WISPs Hex Dissolved' in filename:
                if state_name not in file_groups['wisps_hex']:
                    file_groups['wisps_hex'][state_name] = []
                file_groups['wisps_hex'][state_name].append(full_path)
            elif 'WISPs Dissolved' in filename and 'Hex' not in filename:
                if state_name not in file_groups['wisps_regular']:
                    file_groups['wisps_regular'][state_name] = []
                file_groups['wisps_regular'][state_name].append(full_path)
            elif any(keyword in filename.lower() for keyword in ['antenna', 'tower', 'fcc']):
                if state_name not in file_groups['antenna_files']:
                    file_groups['antenna_files'][state_name] = []
                file_groups['antenna_files'][state_name].append(full_path)
            elif 'cbrs' in filename.lower():
                file_groups['cbrs_files'][state_name] = full_path
            else:
                file_groups['other'].append(full_path)

        return file_groups

    def _extract_state_name(self, filename, full_path):
        """Extract state name from filename or path"""
        # Try to extract from filename first
        if 'State Outline' in filename:
            state_name = filename.replace(' State Outline.sqlite', '')
        elif 'County Outline' in filename:
            state_name = filename.replace(' County Outline.sqlite', '')
        elif 'BEAD' in filename:
            # Extract state name before BEAD keyword
            parts = filename.split()
            for i, part in enumerate(parts):
                if 'BEAD' in part and i > 0:
                    state_name = ' '.join(parts[:i])
                    break
            else:
                state_name = 'Unknown'
        elif 'WISPs' in filename:
            state_name = filename.split(' WISPs')[0]
        elif 'CBRS' in filename:
            state_name = filename.replace(' CBRS.xlsx', '')
        else:
            # If can't extract from filename, try from path
            path_parts = full_path.replace('\\', '/').split('/')
            state_name = 'Unknown'
            for part in path_parts:
                # Look for numbered state folders like "1- OK BEAD", "12- Indiana"
                if re.match(r'\d+[-\s]', part):
                    state_part = re.sub(r'^\d+[-\s]*', '', part)
                    if 'BEAD' in state_part:
                        state_name = state_part.replace(' BEAD', '')
                    else:
                        state_name = state_part
                    break
                # Look for state names directly
                elif any(state in part for state in ['Oklahoma', 'Texas', 'California', 'Florida', 'Alaska']):
                    state_name = part
                    break

        # Clean up and standardize state names
        state_name = self._clean_state_name(state_name)
        return state_name

    def _clean_state_name(self, state_name):
        """Clean and standardize state names"""
        # Handle specific cases
        if state_name.upper() == 'MISSISSPPI':  # Fix typo
            return 'Mississippi'

        # Skip non-state entities
        skip_entities = ['LEVY COUNTY', 'TARANA MAPS', 'LEVY', 'Unknown']
        if state_name.upper() in [s.upper() for s in skip_entities]:
            return 'SKIP'

        # Convert to title case
        return state_name.title()

    def get_latest_files(self, complete_only=True):
        """Get the most recent version of each file type for each state"""
        latest_files = {}

        # Get all unique states
        all_states = set()
        for category in self.file_groups.values():
            if isinstance(category, dict):
                all_states.update(category.keys())

        # Required files for complete states
        required_categories = ['state_outlines', 'county_outlines', 'bead_eligible', 'grid_analysis']

        for state in all_states:
            if state == 'Unknown':
                continue

            # Check if state is complete (has all required files)
            if complete_only:
                has_all_required = all(state in self.file_groups[cat] for cat in required_categories)
                if not has_all_required:
                    continue

            latest_files[state] = {}

            # Single files per state
            single_file_categories = ['state_outlines', 'county_outlines', 'cbrs_files']

            for category in single_file_categories:
                if state in self.file_groups[category]:
                    latest_files[state][category] = self.file_groups[category][state]

            # WISP folders - handle as special case since they can be lists
            wisp_categories = ['wisps_hex', 'wisps_regular']
            for category in wisp_categories:
                if state in self.file_groups[category]:
                    wisp_files = self.file_groups[category][state]
                    if isinstance(wisp_files, list) and wisp_files:
                        # Get the most recent folder/file
                        latest_file = max(wisp_files, key=lambda x: os.path.getmtime(x) if os.path.exists(x) else 0)
                        latest_files[state][category] = latest_file
                    elif isinstance(wisp_files, str):
                        latest_files[state][category] = wisp_files

            # Multiple files per state - get the most recent
            multi_file_categories = ['bead_eligible', 'grid_analysis', 'antenna_files']

            for category in multi_file_categories:
                if state in self.file_groups[category]:
                    files = self.file_groups[category][state]
                    if files:
                        # Sort by modification time and get the latest
                        latest_file = max(files, key=lambda x: os.path.getmtime(x) if os.path.exists(x) else 0)
                        latest_files[state][category] = latest_file

        return latest_files

    def organize_files(self, dry_run=True, copy_instead_of_move=True, complete_only=True):
        """
        Organize files into the structured format

        Args:
            dry_run: If True, only print what would be done without actually copying files
            copy_instead_of_move: If True, copy files instead of moving them
            complete_only: If True, only process states with all required files
        """
        latest_files = self.get_latest_files(complete_only=complete_only)
        actions = []

        for state, files in latest_files.items():
            if not files:  # Skip states with no files
                continue

            # Create state folder
            state_folder = self.target_base_folder / state

            if not dry_run:
                state_folder.mkdir(parents=True, exist_ok=True)

            # Map file categories to expected filenames
            file_mapping = {
                'state_outlines': f"{state} State Outline.sqlite",
                'county_outlines': f"{state} County Outline.sqlite",
                'bead_eligible': f"{state} BEAD Eligible Locations.sqlite",
                'grid_analysis': f"{state} BEAD Grid Analysis Layer.sqlite",
                'cbrs_files': f"{state} CBRS.xlsx"
            }

            # Handle single files
            for category, expected_filename in file_mapping.items():
                if category in files:
                    source_path = files[category]
                    target_path = state_folder / expected_filename

                    action = f"{'COPY' if copy_instead_of_move else 'MOVE'}: {source_path} -> {target_path}"
                    actions.append(action)

                    if not dry_run:
                        try:
                            if copy_instead_of_move:
                                shutil.copy2(source_path, target_path)
                            else:
                                shutil.move(source_path, target_path)
                        except Exception as e:
                            print(f"Error processing {source_path}: {e}")

            # Handle WISPs folders - copy entire folder with contents
            if 'wisps_hex' in files:
                # Find the actual WISP folder path
                source_wisps_folder = self._find_wisp_folder(files['wisps_hex'], f"{state} WISPs Hex Dissolved")
                target_wisps_folder = state_folder / f"{state} WISPs Hex Dissolved"

                if source_wisps_folder and source_wisps_folder.exists():
                    action = f"{'COPY' if copy_instead_of_move else 'MOVE'} WISPs Hex folder: {source_wisps_folder} -> {target_wisps_folder}"
                    actions.append(action)

                    if not dry_run:
                        try:
                            if target_wisps_folder.exists():
                                shutil.rmtree(target_wisps_folder)
                            if copy_instead_of_move:
                                shutil.copytree(source_wisps_folder, target_wisps_folder)
                            else:
                                shutil.move(str(source_wisps_folder), str(target_wisps_folder))
                        except Exception as e:
                            print(f"Error processing WISPs Hex folder {source_wisps_folder}: {e}")
                else:
                    action = f"CREATE empty WISPs Hex folder: {target_wisps_folder} (source folder not found)"
                    actions.append(action)
                    if not dry_run:
                        target_wisps_folder.mkdir(parents=True, exist_ok=True)

            elif 'wisps_regular' in files:
                # Find the actual WISP folder path
                source_wisps_folder = self._find_wisp_folder(files['wisps_regular'], f"{state} WISPs Dissolved")
                target_wisps_folder = state_folder / f"{state} WISPs Dissolved"

                if source_wisps_folder and source_wisps_folder.exists():
                    action = f"{'COPY' if copy_instead_of_move else 'MOVE'} WISPs folder: {source_wisps_folder} -> {target_wisps_folder}"
                    actions.append(action)

                    if not dry_run:
                        try:
                            if target_wisps_folder.exists():
                                shutil.rmtree(target_wisps_folder)
                            if copy_instead_of_move:
                                shutil.copytree(source_wisps_folder, target_wisps_folder)
                            else:
                                shutil.move(str(source_wisps_folder), str(target_wisps_folder))
                        except Exception as e:
                            print(f"Error processing WISPs folder {source_wisps_folder}: {e}")
                else:
                    action = f"CREATE empty WISPs folder: {target_wisps_folder} (source folder not found)"
                    actions.append(action)
                    if not dry_run:
                        target_wisps_folder.mkdir(parents=True, exist_ok=True)

            # Handle antenna files (optional)
            if 'antenna_files' in files:
                antenna_file = files['antenna_files']
                target_path = state_folder / f"{state} FCC Antenna Structures.sqlite"

                action = f"{'COPY' if copy_instead_of_move else 'MOVE'}: {antenna_file} -> {target_path}"
                actions.append(action)

                if not dry_run:
                    try:
                        if copy_instead_of_move:
                            shutil.copy2(antenna_file, target_path)
                        else:
                            shutil.move(antenna_file, target_path)
                    except Exception as e:
                        print(f"Error processing antenna file {antenna_file}: {e}")

            # Create Results folder
            results_folder = state_folder / "Results"
            if not dry_run:
                results_folder.mkdir(exist_ok=True)
            action = f"CREATE Results folder: {results_folder}"
            actions.append(action)

        return actions

    def _find_wisp_folder(self, file_path, expected_folder_name):
        """Find the actual WISP folder given a file path"""
        file_path = Path(file_path)

        # If the file_path is already a folder with the expected name
        if file_path.is_dir() and file_path.name == expected_folder_name:
            return file_path

        # Look in the parent directory for the expected folder
        parent_dir = file_path.parent
        expected_folder = parent_dir / expected_folder_name
        if expected_folder.exists() and expected_folder.is_dir():
            return expected_folder

        # Look for any folder containing "WISPs" in the same directory
        for item in parent_dir.iterdir():
            if item.is_dir() and "WISPs" in item.name and expected_folder_name.split()[0] in item.name:
                return item

        return None

    def generate_report(self, complete_only=False):
        """Generate a report of what files were found for each state"""
        latest_files = self.get_latest_files(complete_only=complete_only)

        print("=" * 80)
        print("BEAD FILE ORGANIZATION REPORT")
        print("=" * 80)

        for state in sorted(latest_files.keys()):
            files = latest_files[state]
            if not files:
                continue

            print(f"\n{state.upper()}:")
            print("-" * 40)

            required_files = ['state_outlines', 'county_outlines', 'bead_eligible', 'grid_analysis']
            optional_files = ['wisps_hex', 'wisps_regular', 'cbrs_files', 'antenna_files']

            # Check required files
            missing_required = []
            for req_file in required_files:
                if req_file in files:
                    print(f"  ✓ {req_file}: {os.path.basename(files[req_file])}")
                else:
                    missing_required.append(req_file)
                    print(f"  ✗ {req_file}: MISSING")

            # Check optional files
            for opt_file in optional_files:
                if opt_file in files:
                    print(f"  + {opt_file}: {os.path.basename(files[opt_file])}")

            if missing_required:
                print(f"  WARNING: Missing required files: {', '.join(missing_required)}")

        # Summary
        total_states = len([s for s in latest_files.keys() if latest_files[s]])
        complete_states = len([s for s in latest_files.keys()
                               if all(req in latest_files[s] for req in required_files)])

        print(f"\n" + "=" * 80)
        print(f"SUMMARY: {complete_states}/{total_states} states have all required files")
        print("=" * 80)


def main():
    # Configuration
    FOLDER_REPORT_CSV = r"C:\Users\meloy\SW2020 Dropbox\SW2020\Workspaces\Narek_Meloyan\Tarana Project\folder_report.csv"
    TARGET_BASE_FOLDER = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data"
    cTARGET_BASE_FOLDER = r"C:\Users\meloy\PycharmProjects\WebGisGeneratorV1\Data"

    # Initialize organizer
    organizer = BEADFileOrganizer(FOLDER_REPORT_CSV, TARGET_BASE_FOLDER)

    # Generate report for complete states only
    print("COMPLETE STATES READY FOR ORGANIZATION:")
    organizer.generate_report(complete_only=True)

    # Show what would be done (dry run) for complete states only
    print("\n" + "=" * 80)
    print("DRY RUN - PROPOSED ACTIONS FOR COMPLETE STATES:")
    print("=" * 80)

    actions = organizer.organize_files(dry_run=True, copy_instead_of_move=True, complete_only=True)
    for i, action in enumerate(actions[:30]):  # Show first 30 actions
        print(f"{i + 1:3d}. {action}")

    if len(actions) > 30:
        print(f"... and {len(actions) - 30} more actions")

    print(f"\nTotal actions for complete states: {len(actions)}")

    # Ask user if they want to proceed
    print("\n" + "=" * 80)
    print("READY TO ORGANIZE FILES:")
    print("=" * 80)
    print("✅ This will organize 38 complete states")
    print("✅ Files will be COPIED (not moved) to preserve originals")
    print("✅ Latest versions will be selected automatically")
    print("⚠️  To actually run organization, change dry_run=False below")

    # UNCOMMENT THE LINE BELOW TO ACTUALLY ORGANIZE FILES:
    organizer.organize_files(dry_run=False, copy_instead_of_move=True, complete_only=True)


if __name__ == "__main__":
    main()