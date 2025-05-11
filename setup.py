from setuptools import setup, find_packages

with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

requirements = [
    "smartapi-python>=1.5.2",
    "pyotp>=2.8.0",
    "websocket-client>=1.8.0",
    "logzero>=1.7.0",
    "requests>=2.31.0",
    "pycryptodome>=4.9.0",
    "pandas>=1.3.0",
    "numpy>=1.20.0",
    "pytz>=2023.3",
    "clickhouse-driver>=0.2.4",
    "python-dateutil>=2.8.2",
]

setup(
    name="commodities-data-fetcher",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Fetch commodity market data and store in ClickHouse",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/commodities-data-fetcher",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "fetch-commodities=scripts.fetch_data:main",
            "setup-commodities-db=scripts.create_tables:main",
        ],
    },
)