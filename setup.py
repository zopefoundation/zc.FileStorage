name, version = 'zc.FileStorage', '0'

from setuptools import setup, find_packages
from distutils.core import Extension
entry_points = """
[console_scripts]
snapshot-in-time = zc.FileStorage.snapshotintime:main
"""

tests_requirements = [
    'zope.testing',
    'mock',
]

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
                        'ZODB<5',
                        'ZEO<5',
                        'transaction<2',
                        ],
    tests_require=tests_requirements,
    extras_require=dict(
        test=tests_requirements,
    ),
    test_suite="zc.FileStorage.tests.test_suite",
    include_package_data = True,
    zip_safe = False,
    entry_points = entry_points,
    )
