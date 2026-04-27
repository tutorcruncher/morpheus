"""Standalone packaging for the rendering submodule, published as `morpheus-mail` on PyPI.

This setup.py lives in its own subdirectory so the root pyproject.toml (which defines
the main morpheus app) doesn't interfere with the build. It packages `app/render` from
the repo root as the `morpheus.render` namespace.

Build with: cd packaging/morpheus-mail && python setup.py sdist bdist_wheel
"""

from setuptools import setup

VERSION = '2.0.0'

setup(
    name='morpheus-mail',
    version=VERSION,
    description='Email rendering engine from morpheus',
    long_description=(
        'Note: this only installs the rendering logic for '
        '`morpheus <https://github.com/tutorcruncher/morpheus>` '
        'for testing and email preview.\n\n'
        'Everything else is excluded to avoid installing unnecessary packages.'
    ),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.12',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Unix',
        'Operating System :: POSIX :: Linux',
        'Topic :: Internet',
    ],
    author='TutorCruncher',
    author_email='tom@tutorcruncher.com',
    url='https://github.com/tutorcruncher/morpheus',
    license='MIT',
    packages=['morpheus.render'],
    package_dir={'morpheus.render': '../../app/render'},
    python_requires='>=3.10',
    zip_safe=True,
    install_requires=[
        'chevron>=0.11.1',
        'libsass>=0.13.2',
        'misaka>=2.1.1',
    ],
)
