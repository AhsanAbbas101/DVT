from loader import Loader
import daft

loader = Loader()

csv_path = 'http://host.docker.internal:4566/test-bucket/sales.csv'
img_path = 'http://host.docker.internal:4566/test-bucket/00955.png'

df = loader.load_csv(csv_path)
print(df)
print(df.count_rows())

df = daft.from_pydict({
    "image_urls": [img_path]
})
df = df.with_column("image_data", df["image_urls"].url.download())
df.show()