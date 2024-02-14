from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="arcspawner",
    entry_points={
        "console_scripts": ["arcspawner-singleuser=arcspawner.singleuser:main"],
    },
    packages=["arcspawner"],
    version="1.3.0.dev",
    description="""arcspawner: A spawner for Jupyterhub to spawn notebooks using batch resource managers.""",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Michael Milligan, Andrea Zonca, Mike Gilbert",
    author_email="milligan@umn.edu",
    url="http://jupyter.org",
    license="BSD",
    platforms="Linux, Mac OS X",
    keywords=["Interactive", "Interpreter", "Shell", "Web", "Jupyter"],
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    project_urls={
        "Bug Reports": "https://github.com/cta-epfl/arcspawner/issues",
        "Source": "https://github.com/cta-epfl/arcspawner/",
        "About Jupyterhub": "http://jupyterhub.readthedocs.io/en/latest/",
        "Jupyter Project": "http://jupyter.org",
    },
    python_requires=">=3.6",
    install_require={
        "jinja2",
        "jupyterhub>=1.5.1",
        "asyncssh==2.14.2",
    },
    extras_require={
        "test": [
            "pytest",
            "pytest-asyncio",
            "pytest-cov",
            "notebook",
        ],
    },
)
