import daft
from daft import DataType

from PIL import Image

import io


import cv2
import numpy as np

@daft.udf(return_dtype=DataType.fixed_size_list(dtype=DataType.int64(), size=2))
def image_dimension(image_bytes):

  def get_dimension(bytes):
    try:
        img = Image.open(io.BytesIO(bytes))
        dim = [img.width, img.height]
    except Exception:
        dim = None
    return dim

  return [ get_dimension(img) for img in image_bytes ]

@daft.udf(return_dtype=DataType.float32())
def image_blur_var(image_bytes):
   
  def get_variance(bytes):
    try:
      np_arr = np.frombuffer(bytes, np.uint8)
      img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
      if img is None:
         return None
      
      gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)  # Convert the image to grayscale
      laplacian = cv2.Laplacian(gray, cv2.CV_32F)  # Compute Laplacian 
      variance = laplacian.var() # Compute variance of the Laplacian
      return variance
    except Exception as e:
      print(e)
      return None
    
  return [ get_variance(img) for img in image_bytes ]


@daft.udf(return_dtype=daft.DataType.int32())
class DetectFace:

  def __init__(self):
    self.face_classifier = cv2.CascadeClassifier(
      cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
    )
  def __call__(self, images_bytes):
    return [self._detect_faces(img) for img in images_bytes]
  
  def _detect_faces(self, bytes_img):
      if bytes_img is None:
         return None
      
      np_arr = np.frombuffer(bytes_img, np.uint8)
      img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

      if img is None:
         return None
      
      gray_image = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)  
      faces = self.face_classifier.detectMultiScale(
        gray_image, scaleFactor=1.1, minNeighbors=5, minSize=(40, 40)
      )

      return len(faces)
  