from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="dneutral-sniper",
    version="0.1.0",
    author="DNEUTRAL Team",
    author_email="support@dneutral.io",
    description="Automated delta hedging for options portfolios on Deribit",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/dneutral-sniper",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "aiohttp>=3.8.0",
        "numpy>=1.20.0",
        "pandas>=1.3.0",
        "click>=8.0.0",
        "tabulate>=0.8.9",
        "pydantic>=1.9.0",
        "python-dotenv>=0.19.0",
    ],
    entry_points={
        "console_scripts": [
            "dneutral=dneutral_sniper.cli:cli",
        ],
    },
    include_package_data=True,
    package_data={
        "dneutral_sniper": ["py.typed"],
    },
)
