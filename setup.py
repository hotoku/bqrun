#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    f.strip() for f in open("./requirements.txt").readlines()
]

setup_requirements = []

test_requirements = []

setup(
    author="Yasunori Horikoshi",
    author_email='horikoshi.et.al@gmail.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Query runner for BigQuery. It automatically analyzes dependencies and runs only necessary queries in parallel.",
    entry_points={
        'console_scripts': [
            'bqrun=bqrun.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='bigquery',
    name='bqrun',
    packages=find_packages(include=['bqrun', 'bqrun.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/hotoku/bqrun',
    version='1.3.1',
    zip_safe=False,
)
