from setuptools import setup, find_packages
from distutils.core import Extension

name = 'zc.FileStorage'
setup(
    name = name,
    version = '0.1dev',
    author = 'Jim Fulton',
    author_email = 'jim@zope.com',
    description = 'New file-storage pack.',
    license = 'ZPL 2.1',

    packages = find_packages('src'),
    ext_modules=[
        Extension('zc.FileStorage._zc_FileStorage_posix_fadvise',
                  ['src/zc/FileStorage/_zc_FileStorage_posix_fadvise.c']),
        Extension('zc.FileStorage._ILBTree',
                  ['src/zc/FileStorage/_ILBTree.c'],
                  include_dirs=['3.8/src'],
                  ),
        ],
    namespace_packages = ['zc'],
    package_dir = {'': 'src'},
    install_requires = [
    'setuptools', 'ZODB3'],
    include_package_data = True,
    zip_safe = False,
    )
