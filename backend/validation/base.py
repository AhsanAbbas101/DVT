"""
Base classes for the generic detector system.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import daft
from daft import col


class BaseDetector(ABC):
    """
    Abstract base class for all detectors.
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.on_column = config.get('on_column')
        
    @abstractmethod
    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        """
        Apply the detector to the dataframe and return the modified dataframe.
        """
        pass
    
    @abstractmethod
    def get_supported_constraints(self) -> List[str]:
        """
        Return list of constraint types supported by this detector.
        """
        pass
    
    def _add_validation_column(self, df: daft.DataFrame, column_name: str, expression: Any) -> daft.DataFrame:
       
        validation_col = f'__VALID_{self.name}_{column_name}_{self.on_column}__'
        return df.with_column(validation_col, expression)


class ConstraintEvaluator:
    """
    Flexible constraint evaluation system.
    """
    
    @staticmethod
    def evaluate_constraint(column_expr: Any, constraint_type: str, value: Any, **kwargs) -> Any:
        
        constraint_type = constraint_type.upper()
        
        if constraint_type == 'LESS_THAN':
            return column_expr < value
        elif constraint_type == 'GREATER_THAN':
            return column_expr > value
        elif constraint_type == 'EQUAL':
            return column_expr == value
        elif constraint_type == 'NOT_EQUAL':
            return column_expr != value
        elif constraint_type == 'BETWEEN':
            min_val, max_val = value if isinstance(value, (list, tuple)) else (kwargs.get('min'), kwargs.get('max'))
            return (column_expr >= min_val) & (column_expr <= max_val)
        elif constraint_type == 'IN':
            return column_expr.is_in(value)
        elif constraint_type == 'NOT_IN':
            return ~column_expr.is_in(value)
        elif constraint_type == 'IS_NULL':
            return column_expr.is_null()
        elif constraint_type == 'IS_NOT_NULL':
            return column_expr.not_null()
        elif constraint_type == 'CONTAINS':
            return column_expr.str.contains(str(value))
        elif constraint_type == 'STARTS_WITH':
            return column_expr.str.startswith(str(value))
        elif constraint_type == 'ENDS_WITH':
            return column_expr.str.endswith(str(value))
        elif constraint_type == 'REGEX_MATCH':
            return column_expr.str.match(str(value))
        elif constraint_type == 'WITHIN_TOLERANCE':
            expected = kwargs.get('expected', 0)
            tolerance = kwargs.get('tolerance', 0.1)
            return abs(column_expr - expected) <= tolerance
        else:
            raise ValueError(f"Unsupported constraint type: {constraint_type}")


class DetectorRegistry:
    """
    Registry for managing detector plugins.
    """
    
    def __init__(self):
        self._detectors = {}
        
    def register(self, detector_type: str, detector_class: type):
        
        if not issubclass(detector_class, BaseDetector):
            raise ValueError(f"Detector class must inherit from BaseDetector")
        
        self._detectors[detector_type.upper()] = detector_class
        
    def get_detector(self, detector_type: str) -> Optional[type]:
        
        return self._detectors.get(detector_type.upper())
    
    def list_detectors(self) -> List[str]:
        
        return list(self._detectors.keys())
    
    def create_detector(self, detector_config: Dict[str, Any]) -> BaseDetector:
        
        detector_type = detector_config.get('type')
        if not detector_type:
            raise ValueError("Detector configuration must include 'type' field")
        
        detector_class = self.get_detector(detector_type)
        if not detector_class:
            raise ValueError(f"Unknown detector type: {detector_type}")
        
        return detector_class(detector_config.get('name', detector_type), detector_config)


# Global registry instance
registry = DetectorRegistry()
