"""
Detector plugins
"""

from validation.base import registry

# Import all detector modules to trigger registration
from .numeric import NumericDetector, IntegerDetector, FloatDetector
from .text import TextDetector, CategoryDetector
from .image import (
    ImageResolutionDetector, ImageBlurDetector, ImageAspectRatioDetector,
    ImageFaceCountDetector, ImageFormatDetector, ImageSizeDetector
)
from .audio import AudioDetector

# Register numeric detectors
registry.register('NUMERIC', NumericDetector)
registry.register('INTEGER', IntegerDetector)
registry.register('FLOAT', FloatDetector)

# Register text detectors
registry.register('TEXT', TextDetector)
registry.register('CATEGORY', CategoryDetector)

# Register image detectors
registry.register('IMAGE_RESOLUTION', ImageResolutionDetector)
registry.register('IMAGE_BLUR', ImageBlurDetector)
registry.register('IMAGE_ASPECT_RATIO', ImageAspectRatioDetector)
registry.register('IMAGE_FACE_COUNT', ImageFaceCountDetector)
registry.register('IMAGE_FORMAT', ImageFormatDetector)
registry.register('IMAGE_SIZE', ImageSizeDetector)

# Register audio detectors
registry.register('AUDIO', AudioDetector)

# Export registry for easy access
__all__ = ['registry']
