import daft
from PIL import Image
import io

class Detector:

  def __init__(self, data, detectors):
    self._data = data
    self._detectors = detectors

  def detect_issues(self):
    
    for detect in self._detectors:
      print(detect)
      if detect['type'] == 'IMAGE_HEIGHT':
        self._validate_img_height(detect)
  

  ### Detectors


  def get_image_dimensions(self,image_bytes):
      if image_bytes is None:
          return (None, None)
      try:
          img = Image.open(io.BytesIO(image_bytes))
          return (img.width, img.height)
      except:
          return (None, None)
    
  def _validate_img_height(self,detector):
    col_name = f'{detector['on_column']}_bytes'
    df =  self._data.with_column(col_name, self._data[detector['on_column']].url.download())
    #df = df.with_column('image_height', daft.col(col_name).image.decode())
    df = df.with_columns({
        "width": daft.col(col_name).apply(lambda x: self.get_image_dimensions(x)[0], return_dtype=daft.DataType.python()),
        "height": daft.col(col_name).apply(lambda x: self.get_image_dimensions(x)[1], return_dtype=daft.DataType.python())
    })
    df.show()

    min_height = detector['constraints'][0]['value']
        # Add column indicating if height is less than threshold
    df = df.with_column(
        "height_too_small",
        daft.col("height") < min_height
    )
    
    # Add reason column for problematic rows
    df = df.with_column(
        "issue_reason",
        daft.col("height").is_null().if_else(
            "download_failed",
            daft.col("height_too_small").if_else(
                f"height_less_than_{min_height}",
                "valid_height"
            )
        )
    )

    df.show()