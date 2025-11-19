# Data Validation Toolkit (DVT) for Multimodal Dataset



## Running the `main.py` Script

The `main.py` script processes CSV files with detectors and generates a validation report.

### Command-Line Arguments
- `--csv`: List of CSV file paths (local or S3). **Required**.
- `--config`: Path to the detector YAML file. **Required**.
- `--s3_endpoint`: Optional S3 endpoint URL for `daft.io.S3Config`. Defaults to `None`.
- `--join_on`: Optional column name to join CSVs on. Defaults to `None`.
- `--report`: Path to save the validation report. Defaults to `./validation_report.txt`.

### Example Usage

```bash
python main.py \
  --csv .\data\structured.csv s3://bucket/unstructured.csv \
  --config .\detectors.yml \
  --s3_endpoint http://localhost:4566 \
  --join_on column_name \
  --report .\output\report.txt
```


## Running the Application

To start the application using Docker Compose, run:

```
docker compose -f ./docker-compose.dev.yml up
```