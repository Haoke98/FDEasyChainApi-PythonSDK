from setuptools import setup, find_packages

setup(
    name="FDEasyChainSDK",
    version="1.0.0",
    author="Sadam·Sadik",
    author_email="Haoke98@outlook.com",
    description="五度易链SDK - 企业数据查询接口",
    license="MIT",
    packages=find_packages(),
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/FDEasyChainSDK",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "requests>=2.25.0",
        "python-dotenv>=0.19.0",
        "colorlog>=6.7.0"
    ],
) 