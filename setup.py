#!/usr/bin/env python3
# Copyright (C) 2015 Job Snijders <job@instituut.net>
#
# This file is part of IRRTree
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from irrtree import __version__

import codecs
import os
import sys

if not sys.version_info[0] >= 3 and not sys.version_info[1] >= 7:
    print("ERROR for irrtree: Sorry, only python 3.7 or higher are supported")
    sys.exit(1)

from setuptools import setup, find_packages
from os.path import abspath, dirname, join

here = abspath(dirname(__file__))

with codecs.open(join(here, 'README.md'), encoding='utf-8') as f:
    README = f.read()

if sys.argv[-1] == 'publish':
    os.system('python3.7 setup.py sdist upload')
    print("You probably want to also tag the version now:")
    print("  git tag -a %s -m 'version %s'" % (__version__, __version__))
    print("  git push --tags")
    sys.exit()

setup(
    name='irrtree',
    version=__version__,
    maintainer="Job Snijders",
    maintainer_email='job@instituut.net',
    url='https://github.com/job/irrtree',
    description='IRRTree',
    long_description=README,
    long_description_content_type="text/markdown",
    license='BSD 2-Clause',
    keywords='irr python bgp routing networking',
    classifiers=[
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.7',
    ],
    install_requires=['asciitree==0.3.3', 'progressbar2'],
    setup_requires=['asciitree==0.3.3', 'progressbar2'],
    packages=find_packages(exclude=['tests', 'tests.*']),
    entry_points={'console_scripts': ['irrtree = irrtree.scripts.cli:main', 'irrtree_parse = irrtree.scripts.parse_irrtree_file:main']},
)
