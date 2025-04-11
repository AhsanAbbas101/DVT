import daft
from daft import col

from .udfs.image import *

class Detector:

  def __init__(self, data, detectors):
    self._data = data
    self._detectors = detectors

  def add_row_hash(self, data: daft.DataFrame):
    return data.with_column(
      '__ROW_HASH__',
      daft.list_(*data.column_names).hash()
    )

  def detect_issues(self):
    
    df = self.add_row_hash(self._data)

    for detector in self._detectors:
      match detector['name']:

        case 'IMAGE_HEIGHT':
          df = self._detector_img_resolution(df, detector, idx=1)
        case 'IMAGE_WIDTH':
          df = self._detector_img_resolution(df, detector, idx=0)
        case 'IMAGE_BLUR':
          df = self._detector_img_blur(df, detector)
        case 'IMAGE_ASPECT_RATIO':
          df = self._detector_img_aspect_ratio(df,detector)
        case 'IMAGE_FACE_COUNT':
          df = self._detector_img_face_count(df,detector)
      
        case _:
          print(f'[Warning] No implementation for \'{detector['name']}\'detector ')
  

  ### Detectors
    
  def _detector_img_resolution(self,df,detector, idx=0):
    bytes_col = f'__{detector['on_column']}_BYTES__'
    resolution_col = f'__{detector['on_column']}_RESOLUTION__'

    if bytes_col not in df:
      df = df.with_column(bytes_col, col(detector['on_column']).url.download(on_error='null'))

    if resolution_col not in df:
      df = df.with_column(resolution_col, image_dimension(col(bytes_col)))


    for constraint in detector['constraints']:

      df = df.with_column(
        f'__VALID_{detector['name']}_{constraint['type']}_{detector['on_column']}__',
        self._get_constraint_expr(col(resolution_col).list.get(idx, None), constraint['type'], constraint['value'] )
        )

    # min_height = detector['constraints'][0]['value']
    #     # Add column indicating if height is less than threshold
    # df = df.with_column(
    #     "height_too_small",
    #     daft.col("height") < min_height
    # )
    
    # # Add reason column for problematic rows
    # df = df.with_column(
    #     "issue_reason",
    #     daft.col("height").is_null().if_else(
    #         "download_failed",
    #         daft.col("height_too_small").if_else(
    #             f"height_less_than_{min_height}",
    #             "valid_height"
    #         )
    #     )
    # )
    #df.explain(show_all=True)
    #df.show()
    return df

  def _detector_img_blur(self,df,detector):
    bytes_col = f'__{detector['on_column']}_BYTES__'
    if bytes_col not in df:
      df = df.with_column(bytes_col, col(detector['on_column']).url.download(on_error='null'))

    blur_var_col = f'__VALID_{detector['name']}_{detector['on_column']}__'
    df = df.with_column(
      blur_var_col, 
      image_blur_var(col(bytes_col)) >= detector['threshold']
      )

    return df

  def _detector_img_aspect_ratio(self, df, detector):
    bytes_col = f'__{detector['on_column']}_BYTES__'
    resolution_col = f'__{detector['on_column']}_RESOLUTION__'
    
    if bytes_col not in df:
      df = df.with_column(bytes_col, col(detector['on_column']).url.download(on_error='null'))   
    
    if resolution_col not in df:
      df = df.with_column(resolution_col, image_dimension(col(bytes_col)))

    aspect_col = f'__{detector['on_column']}_ASPECT_RATIO__'
    df = df.with_column(
      aspect_col,
      col(resolution_col).list.get(0, None) / col(resolution_col).list.get(1, None) 
    )

    df = df.with_column(
      f'__VALID_{detector['name']}_{detector['on_column']}__', 
      abs(col(aspect_col) - detector['expected']) <= detector['tolerance']
      )
        
    return df

  def _detector_img_face_count(self,df, detector):
    bytes_col = f'__{detector['on_column']}_BYTES__'
    if bytes_col not in df:
      df = df.with_column(bytes_col, col(detector['on_column']).url.download(on_error='null'))

    faces_col = f'__VALID_{detector['name']}_{detector['on_column']}__'
    df = df.with_column(
      faces_col, 
      DetectFace(col(bytes_col)) == 0
      )

    return df


  def _get_constraint_expr(self, column, const_type, value):
    if const_type == 'LESS_THAN':
      return column < value
    if const_type == 'GREATER_THAN':
      return column > value