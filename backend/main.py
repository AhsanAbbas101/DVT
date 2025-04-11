# from loader import Loader
from profile import Profile # type: ignore
from detector import Detector
import daft
import yaml

csv_path = './data/test.csv'
io_config = daft.io.IOConfig(s3=daft.io.S3Config(endpoint_url='http://localstack:4566/', anonymous=True))
with open('./data/detectors.yml', 'r') as file:
    detectors = yaml.safe_load(file)

# Load data
profile = Profile(csv_path, io_config=io_config, detectors=detectors)
print(profile._schema)

detector = Detector(profile._data, profile._detectors) 

detector.detect_issues()


# loader = Loader()

# csv_path = 'http://host.docker.internal:4566/test-bucket/sales.csv'
# img_path = 'http://host.docker.internal:4566/test-bucket/00955.png'

# csv_path = 'http:/localstack:4566/test-bucket/sales.csv'
# df = loader.load_csv(csv_path)
# print(df)
# print(df.count_rows())
# print(df.schema())
# for col in df.schema():
#     print()
# print({ col.name : col.dtype for col in df.schema()})
# print(df.columns)
# print(df.column_names)

# io_config = daft.io.IOConfig(s3=daft.io.S3Config(endpoint_url='http://localstack:4566/'))
# df = daft.from_glob_path('s3://test-bucket/**/*.png',io_config)
# print(df.show())
# df = daft.from_pydict({
#     "image_urls": [img_path]
# })
# df = df.with_column("image_data", df["image_urls"].url.download())
# df.show()