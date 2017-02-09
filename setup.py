# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (C) 2017, Ryan P. Wilson
#
#      Authority FX, Inc.
#      www.authorityfx.com

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()
with open('LICENSE') as f:
    license = f.read()

setup(
    name='afxthreads',
    version='0.1.0',
    description="Fully featured multithreading classes.",
    long_description=readme,
    author='Ryan P. Wilson',
    author_email='rpw@authorityfx.com',
    url='https://github.com/AuthorityFX/afxthreads',
    license=license,
    packages=find_packages(exclude=('tests'))
)