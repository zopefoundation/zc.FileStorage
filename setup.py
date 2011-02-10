name, version = 'zc.FileStorage', '0'

from setuptools import setup, find_packages
from distutils.core import Extension

setup(
    name = name,
    version = version,
    author = 'Jim Fulton',
    author_email = 'jim@zope.com',
    description = 'New file-storage pack hack.',
    license = 'ZPL 2.1',

    packages = find_packages('src'),
    ext_modules=[
        Extension('zc.FileStorage._zc_FileStorage_posix_fadvise',
                  ['src/zc/FileStorage/_zc_FileStorage_posix_fadvise.c']),
        ],
    namespace_packages = ['zc'],
    package_dir = {'': 'src'},
    install_requires = ['setuptools',
                        'ZODB3 >=3.9dev'
                        ],
    extras_require=dict(
        test=[
            'zope.testing',
            ]),
    include_package_data = True,
    zip_safe = False,
    )
