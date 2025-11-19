"""
Image detectors.
"""
from typing import Any, Dict, List
import daft
from daft import col

from validation.base import BaseDetector, ConstraintEvaluator
from udfs.image import image_dimension, image_blur_var, DetectFace


class ImageDetector(BaseDetector):
    """
    Base detector for image data validation.
    """

    def _ensure_image_bytes(self, df: daft.DataFrame) -> daft.DataFrame:
        """
        Ensure image bytes column exists for processing.
        """
        bytes_col = f'__{self.on_column}_BYTES__'

        if bytes_col not in df.column_names:
            df = df.with_column(bytes_col, col(self.on_column).url.download(on_error='null'))

        return df, bytes_col

    def get_supported_constraints(self) -> List[str]:
        """
        Return supported constraint types for image data.
        """
        return ['LESS_THAN', 'GREATER_THAN', 'EQUAL', 'NOT_EQUAL', 'BETWEEN', 'WITHIN_TOLERANCE']


class ImageResolutionDetector(ImageDetector):
    """
    Detector for image resolution (width/height) validation.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        df, bytes_col = self._ensure_image_bytes(df)

        # Add resolution column if not exists
        resolution_col = f'__{self.on_column}_RESOLUTION__'
        if resolution_col not in df.column_names:
            df = df.with_column(resolution_col, image_dimension(col(bytes_col)))

        # Determine if checking width (index 0) or height (index 1)
        dimension_type = self.config.get('dimension', 'width').lower()
        idx = 0 if dimension_type == 'width' else 1

        # Apply constraints
        constraints = self.config.get('constraints', [])
        for i, constraint in enumerate(constraints):
            constraint_type = constraint['type']
            value = constraint['value']

            dimension_expr = col(resolution_col).list.get(idx, None)
            expression = ConstraintEvaluator.evaluate_constraint(dimension_expr, constraint_type, value)

            df = self._add_validation_column(df, f"{dimension_type.upper()}_{constraint_type}_{i}", expression)

        return df


class ImageBlurDetector(ImageDetector):
    """
    Detector for image blur validation using Laplacian variance.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        df, bytes_col = self._ensure_image_bytes(df)

        # Get blur threshold
        threshold = self.config.get('threshold', 100.0)

        # Apply blur detection
        blur_expr = image_blur_var(col(bytes_col)) >= threshold
        df = self._add_validation_column(df, "BLUR", blur_expr)

        return df


class ImageAspectRatioDetector(ImageDetector):
    """
    Detector for image aspect ratio validation.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        df, bytes_col = self._ensure_image_bytes(df)

        # Add resolution column if not exists
        resolution_col = f'__{self.on_column}_RESOLUTION__'
        if resolution_col not in df.column_names:
            df = df.with_column(resolution_col, image_dimension(col(bytes_col)))

        # Calculate aspect ratio (width/height)
        aspect_col = f'__{self.on_column}_ASPECT_RATIO__'
        df = df.with_column(
            aspect_col,
            col(resolution_col).list.get(0, None) / col(resolution_col).list.get(1, None)
        )

        # Get expected ratio and tolerance
        expected = self.config.get('expected', 1.0)
        tolerance = self.config.get('tolerance', 0.1)

        # Apply aspect ratio validation
        aspect_expr = abs(col(aspect_col) - expected) <= tolerance
        df = self._add_validation_column(df, "ASPECT_RATIO", aspect_expr)

        return df


class ImageFaceCountDetector(ImageDetector):
    """
    Detector for face count validation in images.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        df, bytes_col = self._ensure_image_bytes(df)

        # Get expected face count (default: 0 for no faces)
        expected_count = self.config.get('expected_count', 0)

        # Apply face detection
        face_count_expr = DetectFace(col(bytes_col)) == expected_count
        df = self._add_validation_column(df, "FACE_COUNT", face_count_expr)

        return df


class ImageFormatDetector(ImageDetector):
    """
    Detector for image format validation.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        # For format detection, we can work with the URL/path directly
        column = col(self.on_column)

        # Get allowed formats
        allowed_formats = self.config.get('allowed_formats', ['.jpg', '.jpeg', '.png', '.gif'])

        # Create validation expression for file extensions
        format_expressions = []
        for fmt in allowed_formats:
            format_expressions.append(column.str.endswith(fmt.lower()) | column.str.endswith(fmt.upper()))

        # Combine all format checks with OR
        if format_expressions:
            final_expr = format_expressions[0]
            for expr in format_expressions[1:]:
                final_expr = final_expr | expr

            df = self._add_validation_column(df, "FORMAT", final_expr)

        return df


class ImageSizeDetector(ImageDetector):
    """
    Detector for image file size validation.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        df, bytes_col = self._ensure_image_bytes(df)

        # Calculate file size in bytes
        size_col = f'__{self.on_column}_SIZE_BYTES__'
        df = df.with_column(size_col, col(bytes_col).str.length())

        # Apply constraints
        constraints = self.config.get('constraints', [])
        for i, constraint in enumerate(constraints):
            constraint_type = constraint['type']
            value = constraint['value']

            # Convert size units if specified
            if 'unit' in constraint:
                unit = constraint['unit'].upper()
                if unit == 'KB':
                    value *= 1024
                elif unit == 'MB':
                    value *= 1024 * 1024
                elif unit == 'GB':
                    value *= 1024 * 1024 * 1024

            expression = ConstraintEvaluator.evaluate_constraint(col(size_col), constraint_type, value)
            df = self._add_validation_column(df, f"SIZE_{constraint_type}_{i}", expression)

        return df
