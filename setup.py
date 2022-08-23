import os
import posixpath
import shutil
from pathlib import Path

import setuptools
from setuptools import find_packages, setup
from setuptools.command import build_ext

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


def read_requirements():
    requirements = []
    with open('requirements.txt') as file:
        requirements = file.read().splitlines()
    with open('dev-requirements.txt') as file:
        requirements.extend(file.read().splitlines())
    print("Requirements: ", requirements)
    return requirements


# [ref](https://github.com/perwin/pyimfit/blob/master/setup.py)
# Modified cleanup command to remove build subdirectory
# Based on: https://stackoverflow.com/questions/1710839/custom-distutils-commands
class CleanCommand(setuptools.Command):
    description = "custom clean command that forcefully removes dist/build directories"
    user_options = []

    def initialize_options(self):
        self._cwd = None

    def finalize_options(self):
        self._cwd = os.getcwd()

    def run(self):
        assert os.getcwd() == self._cwd, 'Must be in package root: %s' % self._cwd
        os.system('rm -rf ./build ./dist')


setup(
    name='fed',
    version='0.1.0-alpha',
    license='Apache 2.0',
    description='A multiple parties involved execution engine on the top of Ray.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Qing Wang',
    author_email='kingchin1218@gmail.com',
    url='https://github.com/jovany-wang/RayFed',
    packages=find_packages(exclude=('examples', 'tests', 'tests.*')),
    extras_require={'dev': ['pylint']},
)
