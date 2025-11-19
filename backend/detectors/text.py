"""
Text detectors.
"""
import re
from typing import Any, Dict, List
import daft
from daft import col

from validation.base import BaseDetector, ConstraintEvaluator


class TextDetector(BaseDetector):
    """
    Detector for text data validation.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        """
        Apply text validation to the dataframe.
        """
        column = col(self.on_column)

        # Handle basic constraints
        constraints = self.config.get('constraints', [])
        for i, constraint in enumerate(constraints):
            constraint_type = constraint['type']
            value = constraint['value']

            expression = ConstraintEvaluator.evaluate_constraint(column, constraint_type, value)
            df = self._add_validation_column(df, f"{constraint_type}_{i}", expression)

        # Length validation
        if 'length' in self.config:
            length_config = self.config['length']

            if 'exact' in length_config:
                expression = column.str.length() == length_config['exact']
                df = self._add_validation_column(df, "LENGTH_EXACT", expression)

            if 'min' in length_config:
                expression = column.str.length() >= length_config['min']
                df = self._add_validation_column(df, "LENGTH_MIN", expression)

            if 'max' in length_config:
                expression = column.str.length() <= length_config['max']
                df = self._add_validation_column(df, "LENGTH_MAX", expression)

            if 'range' in length_config:
                min_len, max_len = length_config['range']
                expression = (column.str.length() >= min_len) & (column.str.length() <= max_len)
                df = self._add_validation_column(df, "LENGTH_RANGE", expression)

        # Pattern validation
        if 'patterns' in self.config:
            patterns = self.config['patterns']

            for i, pattern_config in enumerate(patterns):
                pattern = pattern_config['pattern']
                match_type = pattern_config.get('type', 'match')

                if match_type == 'match':
                    expression = column.str.match(pattern)
                elif match_type == 'contains':
                    expression = column.str.contains(pattern)
                else:
                    raise ValueError(f"Unsupported pattern match type: {match_type}")

                df = self._add_validation_column(df, f"PATTERN_{i}", expression)

        # Format validation
        if 'format' in self.config:
            format_type = self.config['format']['type'].upper()

            if format_type == 'EMAIL':
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                expression = column.str.match(email_pattern)
                df = self._add_validation_column(df, "EMAIL_FORMAT", expression)

            elif format_type == 'PHONE':
                # Basic phone pattern (can be customized)
                phone_pattern = self.config['format'].get('pattern', r'^\+?[\d\s\-\(\)]{10,}$')
                expression = column.str.match(phone_pattern)
                df = self._add_validation_column(df, "PHONE_FORMAT", expression)

            elif format_type == 'URL':
                url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
                expression = column.str.match(url_pattern)
                df = self._add_validation_column(df, "URL_FORMAT", expression)

            elif format_type == 'CUSTOM':
                custom_pattern = self.config['format']['pattern']
                expression = column.str.match(custom_pattern)
                df = self._add_validation_column(df, "CUSTOM_FORMAT", expression)

        # Character set validation
        if 'charset' in self.config:
            charset_config = self.config['charset']

            if 'allowed_chars' in charset_config:
                allowed = charset_config['allowed_chars']
                # Create regex pattern for allowed characters
                escaped_chars = re.escape(allowed)
                pattern = f'^[{escaped_chars}]*$'
                expression = column.str.match(pattern)
                df = self._add_validation_column(df, "ALLOWED_CHARSET", expression)

            if 'forbidden_chars' in charset_config:
                forbidden = charset_config['forbidden_chars']
                # Check that none of the forbidden characters are present
                for char in forbidden:
                    escaped_char = re.escape(char)
                    expression = ~column.str.contains(escaped_char)
                    df = self._add_validation_column(df, f"NO_FORBIDDEN_CHAR_{ord(char)}", expression)

        return df

    def get_supported_constraints(self) -> List[str]:
        """
        Return supported constraint types for text data.
        """
        return [
            'EQUAL', 'NOT_EQUAL', 'IN', 'NOT_IN', 'CONTAINS', 'STARTS_WITH',
            'ENDS_WITH', 'REGEX_MATCH', 'IS_NULL', 'IS_NOT_NULL'
        ]


class CategoryDetector(TextDetector):
    """
    Specialized detector for categorical data validation.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        """
        Apply categorical validation.
        """
        df = super().detect(df)
        column = col(self.on_column)

        # Valid categories validation
        if 'valid_categories' in self.config:
            valid_cats = self.config['valid_categories']
            expression = column.is_in(valid_cats)
            df = self._add_validation_column(df, "VALID_CATEGORY", expression)

        return df
