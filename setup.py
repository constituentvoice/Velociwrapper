from distutils.core import setup

setup(
    name='Velociwrapper',
    version="2.1.0",
    author='Constituent Voice',
    author_email='chris.brown@constituentvoice.com',
    packages=['velociwrapper'],
    url='https://github.com/constituentvoice/Velociwrapper',
    license='BSD',
    description='Wrapper to create models and collections around Elastic Search',
    long_description=open('README.rst').read(),
    install_requires=['python-dateutil', 'elasticsearch<3', 'six', 'future']
)
