__author__ = 'arun-bonam'


try:
	from setuptools import setup
except ImportError:
	from distutils.core import setup

from setuptools import find_packages

packages = find_packages()

package_data = {
}

requires = [
]

classifiers = [
	'Development Status :: 1 - Beta'
]

setup(
	name='recpiesetl',
	version="1.0",
	description=' ETL jobs in Spark',
	packages=packages,
	package_data=package_data,
	install_requires=requires,
	license='MIT',
	classifiers=classifiers,
)
