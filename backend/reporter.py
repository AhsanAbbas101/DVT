"""
Reporter module for generating validation reports from detector outputs.
"""
import daft
from typing import List, Dict, Any
import os


class Reporter:
    """
    Generates text reports from validation results.
    """

    def __init__(self, df: daft.DataFrame):
        self.df = df
        self.validation_columns = self._identify_validation_columns()

    def _identify_validation_columns(self) -> List[str]:
        return [col for col in self.df.column_names if col.startswith('__VALID_')]

    def generate_report(self, output_path: str) -> None:
        """
        Generate a text report file with validation results for each detector.
        """
        total_rows = 0
        false_rows = {col: 0 for col in self.validation_columns}
        false_indices = {col: [] for col in self.validation_columns}

        # Select validation columns
        df = self.df.select(*self.validation_columns)

        for idx, row in enumerate(df.iter_rows()):
            for column in self.validation_columns:
                if row[column] == False:
                    false_rows[column] += 1
                    false_indices[column].append(idx)
            total_rows += 1

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)

        with open(output_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("DATA VALIDATION REPORT\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Total rows: {total_rows}\n\n")

            if not self.validation_columns:
                f.write("No validation results found.\n")
                return

            for col in self.validation_columns:
                invalid_count = false_rows[col]
                valid_count = total_rows - invalid_count
                percentage_invalid = (invalid_count / total_rows) * 100
                percentage_valid = (valid_count / total_rows) * 100

                # Parse column name: Ignore __VALID_ and the last __, then split based on _
                stripped_col = col[len('__VALID_'):-2]  # Remove __VALID_ prefix and trailing __

                # Write validation column section
                f.write(f"{stripped_col}\n")
                f.write("-" * 80 + "\n")
                f.write(f"Valid rows: {valid_count} ({percentage_valid:.2f}%)\n")
                f.write(f"Invalid rows: {invalid_count} ({percentage_invalid:.2f}%)\n")

                if false_indices[col]:
                    f.write(f"Invalid rows indexes: {false_indices[col]}\n")
                else:
                    f.write("Invalid rows indexes: []\n")

                f.write("\n")

            f.write("=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        print(f"Report generated successfully: {output_path}")


def create_report(df: daft.DataFrame, output_path: str) -> None:

    reporter = Reporter(df)
    reporter.generate_report(output_path)
