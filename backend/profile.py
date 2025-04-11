# type: ignore
import daft
from loader import Loader
import uuid
import datetime
from dataclasses import dataclass

@dataclass
class Profile:
  
  _run_id: uuid.UUID = uuid.uuid4()
  _created = datetime.datetime.now()
  _data: daft.DataFrame = None
  _io_config: daft.io.IOConfig = None
  _loader: Loader = Loader()
  _schema: dict = None
  _detectors = None
  _stats = None

  def __init__(self, path, io_config=None, detectors=None):
    self._path = path
    self._io_config = io_config
    self._detectors = detectors

    self._load_data()
    self._load_schema()
    self._gather_stats()

  def _load_data(self):
    self._data = self._loader.load_csv(self._path, self._io_config)
    
  def _load_schema(self):
    self._schema = { col.name : col.dtype for col in self._data.schema()}

  def _gather_stats(self):
    summary = self._data.summarize().to_pydict()
    
    self._stats = {}
    for i, col in enumerate(summary['column']):

      if not col:
        col = f'unamed_col_{i}'
      
      self._stats[col] = { k : summary[k][i] for k in summary if k != 'column' }
    