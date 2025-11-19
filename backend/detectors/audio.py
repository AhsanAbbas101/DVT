
from typing import Any, Dict, List
import daft
from daft import col
import soundfile as sf
import numpy as np
from validation.base import BaseDetector, ConstraintEvaluator
import io

class AudioDetector(BaseDetector):
    """
    Detector for audio data validation using soundfile.
    """

    def detect(self, df: daft.DataFrame) -> daft.DataFrame:
        column = col(self.on_column)

        # Handle constraints
        constraints = self.config.get('constraints', [])
        for i, constraint in enumerate(constraints):
            constraint_type = constraint['type']
            value = constraint.get('value')

            expression = ConstraintEvaluator.evaluate_constraint(column, constraint_type, value)
            df = self._add_validation_column(df, f"{constraint_type}_{i}", expression)

        # Audio file validation
        if self.config.get('validate_audio_files', True):
            df = self._validate_audio_files(df)

        # Duration validation
        if 'duration_range' in self.config:
            df = self._validate_duration(df)

        # Sample rate validation
        if 'sample_rate' in self.config:
            df = self._validate_sample_rate(df)

        # Channel validation
        if 'channels' in self.config:
            df = self._validate_channels(df)

        return df

    def _ensure_audio_file(self, df: daft.DataFrame) -> daft.DataFrame:
        """
        Ensure audio file column exists for processing.
        """
        bytes_col = f'__{self.on_column}_BYTES__'

        if bytes_col not in df.column_names:
            df = df.with_column(bytes_col, col(self.on_column).url.download(on_error='null'))

        return df, bytes_col

    def _validate_audio_files(self, df: daft.DataFrame) -> daft.DataFrame:
        """Validate that audio files can be read by soundfile."""
        df, bytes_col = self._ensure_audio_file(df)

        def is_valid_audio(file_bytes):
            try:
                if file_bytes is None or file_bytes == b"":
                    return False
                info = sf.info(io.BytesIO(file_bytes))
                return True
            except Exception:
                return False

        validation_expr = col(bytes_col).apply(is_valid_audio, return_dtype=daft.DataType.bool())
        return self._add_validation_column(df, "VALID_AUDIO_FILE", validation_expr)

    def _validate_duration(self, df: daft.DataFrame) -> daft.DataFrame:
        """Validate audio duration is within specified range."""
        df, bytes_col = self._ensure_audio_file(df)

        duration_config = self.config['duration_range']
        min_duration = duration_config.get('min', 0)
        max_duration = duration_config.get('max', float('inf'))

        def get_duration(file_bytes):
            try:
                if file_bytes is None or file_bytes == b"":
                    return 0
                info = sf.info(io.BytesIO(file_bytes))
                return info.duration
            except Exception:
                return 0

        duration_expr = col(bytes_col).apply(get_duration, return_dtype=daft.DataType.float64())
        validation_expr = (duration_expr >= min_duration) & (duration_expr <= max_duration)

        return self._add_validation_column(df, "DURATION_RANGE", validation_expr)

    def _validate_sample_rate(self, df: daft.DataFrame) -> daft.DataFrame:
        """Validate audio sample rate matches expected value."""
        df, bytes_col = self._ensure_audio_file(df)

        expected_rate = self.config['sample_rate']

        def get_sample_rate(file_bytes):
            try:
                if file_bytes is None or file_bytes == b"":
                    return 0
                info = sf.info(io.BytesIO(file_bytes))
                return info.samplerate
            except Exception:
                return 0

        rate_expr = col(bytes_col).apply(get_sample_rate, return_dtype=daft.DataType.int64())
        validation_expr = rate_expr == expected_rate

        return self._add_validation_column(df, "SAMPLE_RATE", validation_expr)

    def _validate_channels(self, df: daft.DataFrame) -> daft.DataFrame:
        """Validate number of audio channels."""
        df, bytes_col = self._ensure_audio_file(df)

        expected_channels = self.config['channels']

        def get_channels(file_bytes):
            try:
                if file_bytes is None or file_bytes == b"":
                    return 0
                info = sf.info(io.BytesIO(file_bytes))
                return info.channels
            except Exception:
                return 0

        channels_expr = col(bytes_col).apply(get_channels, return_dtype=daft.DataType.int64())
        validation_expr = channels_expr == expected_channels

        return self._add_validation_column(df, "CHANNELS", validation_expr)

    def get_supported_constraints(self) -> List[str]:
        """Return supported constraint types for audio data."""
        return [
            'EQUAL', 'NOT_EQUAL', 'IS_NULL', 'IS_NOT_NULL'
        ]
