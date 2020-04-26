from distutils.core import setup
import os

the_lib_folder = os.path.dirname(os.path.realpath(__file__))

requirement_path = the_lib_folder + '/requirements.txt'
install_requires = []
if os.path.isfile(requirement_path):
    with open(requirement_path) as f:
        install_requires = f.read().splitlines()

setup(
    name='py-parquet-builder',
    version='1.0',
    packages=['py_parquet_builder'],
    url='',
    license='',
    author='dillonjohnson',
    author_email='dillonjohnson1015@gmail.com',
    description='This library creates a parquet from an array of dicts.',
    install_requires=install_requires
)
