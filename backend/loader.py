import daft
from typing import List, Optional

class Loader:

  def _is_s3_path(self, path: str) -> bool:
    """
    Check if a path is an S3 endpoint (s3:// or http:// with S3 endpoint pattern).
    """
    return path.startswith('s3://') or path.startswith('http://') or path.startswith('https://')

  def load_csv(self, path: str, io_config: Optional[daft.io.IOConfig] = None):
    """
    Load a CSV file from local path or S3 endpoint.

    """
    try:
      # For S3 paths, io_config should be provided
      if self._is_s3_path(path):
        if io_config is None:
          raise ValueError(f'S3 path detected but io_config is None. Path: {path}')
        return daft.read_csv(path=path, io_config=io_config)
      else:
        return daft.read_csv(path=path, io_config=None)
    except Exception as e:
      raise Exception(f'Error reading csv: {path}. {str(e)}')

  def join_csvs(
    self,
    paths: List[str],
    io_config: Optional[daft.io.IOConfig] = None,
    join_on: Optional[str] = None,
    how: str = 'inner'
  ) -> daft.DataFrame:
    """
    Load and join multiple CSV files.
    """
    if not paths:
      raise ValueError('At least one CSV path must be provided')

    if len(paths) == 1:
      return self.load_csv(paths[0], io_config)

    # Load all CSVs
    dataframes = []
    for path in paths:
      df = self.load_csv(path, io_config)
      dataframes.append(df)

    # Determine join column
    if join_on is None:
      first_df = dataframes[0]
      if not first_df.column_names:
        raise ValueError('First CSV has no columns')
      join_on = first_df.column_names[0]

    # Perform sequential joins
    result = dataframes[0]
    for i, df in enumerate(dataframes[1:], start=1):
      if join_on not in result.column_names:
        raise ValueError(f'Join column "{join_on}" not found in first dataframe')
      if join_on not in df.column_names:
        raise ValueError(f'Join column "{join_on}" not found in dataframe {i+1} (path: {paths[i]})')

      result = result.join(df, on=join_on, how=how)

    return result

  def load_image(self, path: str):
    try:
      return daft.lit(path).url.download()
    except Exception:
      raise Exception('Error reading image: '+path )
