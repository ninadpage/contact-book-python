# encoding=utf-8
# Author: ninadpage

from setuptools import setup

setup(name='contactbook',
      version='0.0.1',
      description='A simple Python implementation of a personal address book',
      url='https://github.com/ninadpage/contact-book-python',
      author='Ninad Page',
      license='MIT',
      packages=['contactbook'],
      package_dir={
          '': 'src',
      },
      test_suite='tests',
      install_requires=[
          'SQLAlchemy==1.0.14',
          'PyTrie==0.2',
      ]
      )
