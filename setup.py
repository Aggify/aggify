from setuptools import setup, find_packages

setup(
    name="aggify",
    version="0.1.2",
    description="A MongoDB aggregation generator for Mongoengine",
    author="SeYeD.Dev",
    author_email="me@seyed.dev",
    url="https://github.com/Aggify/aggify",
    packages=find_packages(),
    install_requires=["mongoengine >= 0.27.0"],
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
    ],
)
