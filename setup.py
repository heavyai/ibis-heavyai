"""Ibis OmniSciDB backend."""
import os

import setuptools

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


setuptools.setup(
    name='ibis-omniscidb',
    description='Ibis OmniSciDB backend',
    long_description=open(os.path.join(BASE_PATH, 'README.md')).read(),
    long_description_content_type="text/markdown",
    url='https://github.com/omnisci/ibis-omniscidb',
    packages=setuptools.find_packages(),
    python_requires='>=3.7',
    install_requires=[
        'ibis-framework',  # TODO: require ibis 2.0 when it's released
        'numba<0.54',
        'pandas',
        'pyomnisci>=0.27.0',
        'pyomniscidb>=5.5.2',
        'pyarrow',
        'rbc-project>=0.4.0',
        'sqlalchemy<1.4',  # TODO: it should be fixed by ibis 2.0
    ],
    setup_requires=['setuptools_scm'],
    use_scm_version=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering',
    ],
    license='Apache Software License',
    maintainer='OmniSci',
    maintainer_email='community@omnisci.com',
)
