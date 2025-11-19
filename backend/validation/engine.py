
import daft
from typing import List, Dict, Any

from .base import registry
from detectors import registry as detector_registry


class Detector:

    def __init__(self, data: daft.DataFrame, detectors: List[Dict[str, Any]]):

        self._data = data
        self._detectors = detectors
        self._registry = registry

    def add_row_hash(self, data: daft.DataFrame) -> daft.DataFrame:

        return data.with_column(
            '__ROW_HASH__',
            daft.list_(*data.column_names).hash()
        )

    def detect_issues(self) -> daft.DataFrame:

        df = self.add_row_hash(self._data)

        for detector_config in self._detectors:
            try:
                # Create detector instance using registry
                detector = self._create_detector_from_config(detector_config)

                # Apply detection
                df = detector.detect(df)

            except Exception as e:
                detector_name = detector_config.get('name', 'unknown')
                print(f'[Error] Failed to run detector \'{detector_name}\': {str(e)}')

        return df

    def detect_and_show(self, num_rows: int = 10) -> None:

        df = self.detect_issues()
        df.show(num_rows)

    def detect_and_save(self, path: str) -> None:
        pass

    def _create_detector_from_config(self, detector_config: Dict[str, Any]):

        detector_type = detector_config.get('type')
        if not detector_type:
            raise ValueError("Detector configuration must include 'type' field")

        return self._registry.create_detector(detector_config)

    def get_available_detectors(self) -> List[str]:

        return self._registry.list_detectors()

    def get_detector_info(self, detector_type: str) -> Dict[str, Any]:

        detector_class = self._registry.get_detector(detector_type)
        if not detector_class:
            return {"error": f"Unknown detector type: {detector_type}"}

        # Create a dummy instance to get supported constraints
        dummy_config = {"name": "dummy", "on_column": "dummy", "type": detector_type}
        try:
            dummy_detector = detector_class("dummy", dummy_config)
            supported_constraints = dummy_detector.get_supported_constraints()
        except Exception:
            supported_constraints = []

        return {
            "type": detector_type,
            "class": detector_class.__name__,
            "module": detector_class.__module__,
            "supported_constraints": supported_constraints,
            "description": detector_class.__doc__ or "No description available"
        }