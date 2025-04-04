import daft
from loader import Loader

class Profile:
  
  _data: daft.DataFrame
  _io_config: daft.io.IOConfig
  _loader: Loader = Loader()
  _schema: dict
  _detectors = [
    {
      "type": 'IMAGE_HEIGHT',
      "on_column" : 'image_path', #fixed
      "constraints": [
        {
          "type": "LESS_THAN",
          "value": 400
        }
      ]
    }
  ]

  @property
  def data(self) -> daft.DataFrame:
    return self._data
  
  @property
  def detectors(self):
    return self._detectors

  def __init__(self, path, io_config=None):
    self._path = path
    self._io_config = io_config

    self._load_data()

  def _load_data(self):
    self._data = self._loader.load_csv(self._path, self._io_config)
    self._schema = { col.name : col.dtype for col in self._data.schema()}

  def get_schema(self):
    return self._schema