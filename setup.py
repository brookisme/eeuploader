from distutils.core import setup
setup(
  name = 'eeuploader',
  packages = ['eeuploader'],
  version = '0.0.0.1',
  description = 'Image Kit: python utilities for working with multispectral imagery',
  author = 'Brookie Guzder-Williams',
  author_email = 'brook.williams@gmail.com',
  url = 'https://github.com/brookisme/eeuploader',
  download_url = 'https://github.com/brookisme/eeuploader/tarball/0.1',
  keywords = ['python','gee','ee','google earth engine','earth engine','raster'],
  include_package_data=True,
  data_files=[
    (
      'config',[]
    )
  ],
  classifiers = [],
  entry_points={
      'console_scripts': [
          'eeuploader=eeuploader.cli:cli'
      ]
  }
)