import daft

class Loader:

  def load_csv(self,path :str, io_config= None):

    try:
      return daft.read_csv(path=path, io_config=io_config)

    except Exception:
      raise Exception('Error reading csv: '+path )

  def load_image(self,path:str):
    try:
      return daft.lit(path).url.download()
    except Exception:
      raise Exception('Error reading image: '+path )  