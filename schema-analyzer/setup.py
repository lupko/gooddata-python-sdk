from setuptools import setup, find_packages

REQUIRES = [
    "JayDeBeApi~=1.2.3",
    "networkx~=2.6.2",
]


def _read_version():
    with open("VERSION", "rt") as version_file:
        return version_file.readline().strip()


setup(
    name="gooddata-sdk",
    description="GoodData.CN Schema Analysis tool",
    version=_read_version(),
    author="GoodData",
    license="MIT",
    install_requires=REQUIRES,
    packages=find_packages(exclude=["tests"]),
    python_requires=">=3.7.0",
    classifiers=[
        "Development Status :: 1 - Planning",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Database",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "Typing :: Typed",
    ],
    keywords=[
        "gooddata",
        "schema",
        "analysis",
        "api",
        "analytics",
        "headless",
        "business",
        "intelligence",
        "headless-bi",
        "cloud",
        "native",
        "semantic",
        "layer",
        "sql",
        "metrics",
    ],
)
