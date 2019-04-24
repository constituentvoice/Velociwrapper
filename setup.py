from distutils.core import setup

# this nastiness to try to get the version without loading the whole module
import sys
import os
sys.path.append(os.path.join(os.path.curdir, 'velociwrapper'))
from version import __version__

setup(
    name='Velociwrapper',
    version=__version__,
    author='Constituent Voice',
    author_email='chris.brown@constituentvoice.com',
    packages=['velociwrapper'],
    url='https://github.com/constituentvoice/Velociwrapper',
    license='BSD',
    description='Wrapper to create models and collections around Elastic Search',
    long_description=open('README.rst').read(),
    install_requires=['python-dateutil', 'elasticsearch<3', 'six', 'future']
)
