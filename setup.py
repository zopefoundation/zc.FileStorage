from setuptools import setup, find_packages

name = 'zc.FileStorage'
setup(
    name = name,
    version = '0.1dev',
    author = 'Jim Fulton',
    author_email = 'jim@zope.com',
    description = 'New file-storage pack.',
    license = 'ZPL 2.1',

    packages = find_packages('src'),
    namespace_packages = ['zc'],
    package_dir = {'': 'src'},
    install_requires = [
    'setuptools', 'ZODB3'],
    include_package_data = True,
    zip_safe = False,
    )
