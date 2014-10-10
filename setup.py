from distutils.core import setup
from distutils.sysconfig import get_python_lib
import os

setup(
	name='Velociwrapper',
	version='0.2.8',
	author='Chris Brown',
	author_email='chris.brown@nwyc.com',
	packages=['velociwrapper'],
	url='https://github.com/constituentvoice/Velociwrapper',
	license='BSD',
	description='Wrapper to create models and collections around Elastic Search',
	long_description=open('README.rst').read(),
	install_requires=['python-dateutil','elasticsearch']
)
