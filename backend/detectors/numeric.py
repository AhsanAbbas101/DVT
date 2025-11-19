"""
Numeric detectors.
"""
from typing import Any, Dict, List
import daft
from daft import col

from validation.base import BaseDetector, ConstraintEvaluator


class NumericDetector(BaseDetector):
    """
    Detector for numeric data validation.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:

        column = col(self.on_column)

        # Handle constraints
        constraints = self.config.get('constraints', [])
        for i, constraint in enumerate(constraints):
            constraint_type = constraint['type']
            value = constraint.get('value')

            # Special handling for BETWEEN constraint
            if constraint_type.upper() == 'BETWEEN':
                min_val = constraint.get('min', value[0] if isinstance(value, (list, tuple)) else value)
                max_val = constraint.get('max', value[1] if isinstance(value, (list, tuple)) else value)
                expression = ConstraintEvaluator.evaluate_constraint(
                    column, constraint_type, value, min=min_val, max=max_val
                )
            else:
                expression = ConstraintEvaluator.evaluate_constraint(column, constraint_type, value)

            df = self._add_validation_column(df, f"{constraint_type}", expression)

        # Handle range validation
        if 'range' in self.config:
            range_config = self.config['range']
            min_val = range_config.get('min')
            max_val = range_config.get('max')

            if min_val is not None and max_val is not None:
                expression = (column >= min_val) & (column <= max_val)
                df = self._add_validation_column(df, "RANGE", expression)
            elif min_val is not None:
                expression = column >= min_val
                df = self._add_validation_column(df, "MIN_RANGE", expression)
            elif max_val is not None:
                expression = column <= max_val
                df = self._add_validation_column(df, "MAX_RANGE", expression)

        # Handle statistical validation
        if 'statistics' in self.config:
            stats_config = self.config['statistics']

            # Z-score outlier detection
            if 'z_score_threshold' in stats_config:
                threshold = stats_config['z_score_threshold']
                mean_val = column.mean()
                std_val = column.std()
                z_score = abs((column - mean_val) / std_val)
                expression = z_score <= threshold
                df = self._add_validation_column(df, "Z_SCORE_OUTLIER", expression)

            # IQR outlier detection
            if 'iqr_multiplier' in stats_config:
                multiplier = stats_config['iqr_multiplier']
                q1 = column.quantile(0.25)
                q3 = column.quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - (multiplier * iqr)
                upper_bound = q3 + (multiplier * iqr)
                expression = (column >= lower_bound) & (column <= upper_bound)
                df = self._add_validation_column(df, "IQR_OUTLIER", expression)

        return df

    def get_supported_constraints(self) -> List[str]:
        """
        Return supported constraint types for numeric data.
        """
        return [
            'LESS_THAN', 'GREATER_THAN', 'EQUAL', 'NOT_EQUAL', 'BETWEEN',
            'IS_NULL', 'IS_NOT_NULL'
        ]


class IntegerDetector(NumericDetector):
    """
    Specialized detector for integer validation.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:

        df = super().detect(df)
        column = col(self.on_column)

        # Check if values are actually integers
        if self.config.get('strict_integer', False):
            expression = (column % 1) == 0
            df = self._add_validation_column(df, "IS_INTEGER", expression)

        return df


class FloatDetector(NumericDetector):
    """
    Specialized detector for float validation.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        df = super().detect(df)
        column = col(self.on_column)

        # Check for infinity values
        if self.config.get('check_infinity', False):
            expression = ~column.is_inf()
            df = self._add_validation_column(df, "NOT_INFINITY", expression)

        # Check for NaN values
        if self.config.get('check_nan', False):
            expression = ~column.is_nan()
            df = self._add_validation_column(df, "NOT_NAN", expression)

        # Decimal precision validation
        if 'decimal_places' in self.config:
            precision = self.config['decimal_places']
            multiplier = 10 ** precision
            expression = ((column * multiplier) % 1) == 0
            df = self._add_validation_column(df, f"DECIMAL_PRECISION_{precision}", expression)

        return df
