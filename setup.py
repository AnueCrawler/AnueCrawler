from distutils.core import setup


import os
import re
from setuptools import setup, find_packages

current_path = os.path.abspath(os.path.dirname(__file__))


def read_file(*parts):
    with open(os.path.join(current_path, *parts), encoding='utf-8') as reader:
        return reader.read()


setup(
    name = 'AnueCrawler',
    packages = ['anuecrawler.news'],
    scripts = [],
    version = '1.0.2',
    description = 'Crawler for CNYes ',
    long_description_content_type='text/markdown',
    long_description=read_file('README'),
    author = 'LanDeveloper',
    author_email = 'nilm987521@gmail.com',
    url = 'https://github.com/LanDeveloper/AnueCrawler',
    download_url = '',
    keywords = ['pypi','cnyes','crawler'],
    classifiers = []
)
