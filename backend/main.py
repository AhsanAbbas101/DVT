import argparse
from profile import Profile  # type: ignore
from validation import Detector
import daft
import yaml
from reporter import create_report


def load_detector_config(config_path):
    """Load and parse the detector YAML configuration file."""
    try:
        with open(config_path, 'r') as file:
            detectors = yaml.safe_load(file)
        return detectors
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file '{config_path}' not found.")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Failed to parse YAML config file: {e}")
    except Exception as e:
        raise Exception(f"Failed to load config file: {e}")

def main():

    parser = argparse.ArgumentParser(description="Process CSV files with detectors.")
    parser.add_argument(
        "--csv",
        nargs="+",
        required=True,
        help="List of CSV file paths (local or S3)."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the detector YAML config file."
    )
    parser.add_argument(
        "--s3_endpoint",
        default=None,
        help="Optional S3 endpoint URL for daft.io.S3Config."
    )
    parser.add_argument(
        "--join_on",
        default=None,
        help="Optional column name to join CSVs on."
    )
    parser.add_argument(
        "--report",
        default="./validation_report.txt",
        help="Path to save the validation report."
    )
    args = parser.parse_args()

    # Load the detector YAML file
    detectors = load_detector_config(args.config)

    # Configure S3 if endpoint is provided
    io_config = None
    if args.s3_endpoint:
        io_config = daft.io.IOConfig(
            s3=daft.io.S3Config(endpoint_url=args.s3_endpoint, anonymous=True)
        )

    print(f"Joining CSVs: {args.csv}")
    print(f"S3 Endpoint: {args.s3_endpoint}")
    print(f"Join on column: {args.join_on}")

    # Create a Profile instance and load data
    profile = Profile(args.csv, io_config=io_config, join_on=args.join_on, detectors=detectors)
    profile._load_data()

    # Run the detector
    detector = Detector(profile._data, profile._detectors)
    df = detector.detect_issues()

    # Generate the report
    create_report(df, args.report)
    print(f"Validation report saved to {args.report}")

if __name__ == "__main__":
    main()

