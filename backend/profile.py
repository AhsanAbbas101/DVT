# type: ignore
import daft
from loader import Loader
import uuid
import datetime
from dataclasses import dataclass
from typing import List, Optional, Union

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

  def __init__(self, path: Union[str, List[str]], io_config: Optional[daft.io.IOConfig] = None, detectors=None, join_on: Optional[str] = None):
    """
    Initialize Profile.
    """
    self._path = path
    self._io_config = io_config
    self._detectors = detectors
    self._join_on = join_on


  def _load_data(self):

    if isinstance(self._path, list):
      self._data = self._loader.join_csvs(
        paths=self._path,
        io_config=self._io_config,
        join_on=self._join_on
      )
    else:
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

    print(self._stats)