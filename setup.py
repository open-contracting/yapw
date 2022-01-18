from setuptools import find_packages, setup

with open("README.rst") as f:
    long_description = f.read()

setup(
    name="yapw",
    version="0.0.7",
    author="Open Contracting Partnership",
    author_email="data@open-contracting.org",
    url="https://github.com/open-contracting/yapw",
    description="A Pika wrapper with error handling, signal handling and good defaults.",
    license="BSD",
    packages=find_packages(exclude=["tests", "tests.*"]),
    long_description=long_description,
    long_description_content_type="text/x-rst",
    install_requires=[
        "pika",
        "typing-extensions;python_version<'3.8'",
    ],
    extras_require={
        "perf": [
            "orjson",
        ],
        "test": [
            "coveralls",
            "pytest",
            "pytest-cov",
        ],
        "types": [
            "mypy",
            "types-orjson",
            "types-pika",
        ],
        "docs": [
            "furo",
            "sphinx",
            "sphinx-autobuild",
        ],
    },
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
)
