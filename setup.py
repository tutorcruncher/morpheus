from setuptools import setup

setup(
    name='morpheus-mail',
    version='0.0.5',
    description='Email rendering engine from morpheus',
    long_description="""
Note: this only installs the rendering logic for `morpheus <https://github.com/tutorcruncher/morpheus>`_ 
for testing and email preview.

Everything else is excluded to avoid installing unnecessary packages. 
""",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Unix',
        'Operating System :: POSIX :: Linux',
        'Topic :: Internet',
    ],
    author='Samuel Colvin',
    author_email='s@muelcolvin.com',
    url='https://github.com/tutorcruncher/morpheus',
    license='MIT',
    packages=['morpheus.render'],
    package_dir={'morpheus.render': 'morpheus/app/render'},
    python_requires='>=3.6',
    zip_safe=True,
    install_requires=[
        'chevron>=0.11.1',
        'libsass>=0.13.2',
        'misaka>=2.1.0',
    ],
)
