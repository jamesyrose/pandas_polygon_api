from setuptools import setup

classifiers = [
    "Development Status :: Beta",
    "Intended Audience :: Financial and Insurance Industry",
    "Programming Language :: Python",
    "Operating System :: OS Independent",
    "Natural Language :: English",
    "License :: MIT",
]

setup(name='pandas_polygon_api',
      version='0.1',
      description='Polygon end point accessed through pandas',
      url='http://github.com/jamesyrose/pandas_polygon_api',
      author='jamesyrose',
      author_email='jyrose@vertet.io',
      keywords=["pandas", "finance", "numpy", "analysis", 'algo trading', 'polygon'],
      license='MIT',
      packages=['pandas_polygon_api'],
      install_requires=['pandas', 'requests'],
      zip_safe=False)