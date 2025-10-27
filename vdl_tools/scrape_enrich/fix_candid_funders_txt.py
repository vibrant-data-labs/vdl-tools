import pandas as pd
import vdl_tools.shared_tools.project_config as pc

#paths = pc.get_paths()
"""This was a one off script to fix a malformed candid funders txt file"""

# -------------------

def clean_csv_file(input_filename: str, output_filename: str):
    """
    Reads a malformed, pipe-delimited CSV file and fixes records that are
    improperly split across multiple lines.

    The function identifies broken lines by checking the number of delimiters
    and merges them into a single, correct line in the output file.

    Args:
        input_filename (str): The path to the malformed input CSV file.
        output_filename (str): The path where the cleaned CSV file will be saved.
    """
    print(f"Attempting to clean '{input_filename}'...")

    try:
        with open(input_filename, 'r', encoding='utf-16') as f_in, \
                open(output_filename, 'w', encoding='utf-16') as f_out:

            # 1. Write the header line without any changes
            header = f_in.readline()
            f_out.write(header)

            # 2. Process the rest of the file line by line
            buffer = ""
            for line in f_in:
                # A "real" line should have at least 2 pipes.
                # If not, it's a continuation of the previous line.
                if line.count('|') < 2:
                    # Append this broken piece to the buffer, replacing the
                    # newline with a space for readability.
                    buffer = buffer.strip('\r\n') + " " + line
                else:
                    # This is a new, complete line.
                    # First, write the fully formed line from the buffer.
                    if buffer:
                        f_out.write(buffer)
                    # Then, start the buffer over with the new line.
                    buffer = line

            # 3. Write the very last buffered line to the file
            if buffer:
                f_out.write(buffer)

        print(f"Successfully created cleaned file: '{output_filename}'")

    except FileNotFoundError:
        print(f"ERROR: The file '{input_filename}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # --- Configuration ---
    original_file = '.././shared-data/data/candid/2025_08_19/VibrantDataLabs_Funders.txt'
    fixed_file = '.././shared-data/data/candid/2025_08_19/candid_funders.csv'
    # Clean the original file and save it to the fixed file path
    clean_csv_file(original_file, fixed_file)

    # Load the cleaned data to verify
    df_funders = pd.read_csv(fixed_file, sep='|', encoding='utf-16')
    print(f"Loaded {len(df_funders)} funders from '{fixed_file}'")
    print(df_funders.head())