import os
import platform
import sys

import setuptools
from setuptools import find_packages, setup

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Default Linux platform tag
plat_name = "manylinux2014_x86_64"

if sys.platform == "darwin":
    # Due to a bug in conda x64 python, platform tag has to be 10_16 for X64 wheel
    if platform.machine() == "x86_64":
        plat_name = "macosx_10_16_x86_64"
    else:
        plat_name = "macosx_11_0_arm64"

def read_requirements():
    requirements = []
    with open('requirements.txt') as file:
        requirements = file.read().splitlines()
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
    name='secretflow-rayfed',
    version='0.1.0a10',
    license='Apache 2.0',
    description='A multiple parties involved execution engine on the top of Ray.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='AntGroup',
    author_email='secretflow-contact@service.alipay.com',
    url='https://github.com/secretflow/rayfed',
    packages=find_packages(exclude=('examples', 'tests', 'tests.*')),
    install_requires=read_requirements(),
    extras_require={'dev': ['pylint']},
    options={'bdist_wheel': {'plat_name': plat_name}},
)
