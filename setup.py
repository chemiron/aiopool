from setuptools import setup, find_packages

setup(
    name="aiopool",
    version="0.0.2a1",
    description="Subprocesses for asyncio",
    long_description="A library for running separated subprocesses with asyncio.",
    url="https://github.com/chemiron/aiopool.git",
    author="Aliaksandr Nasukovich",
    author_email="chemiron34@gmail.com",
    license='MIT',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
	"License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.4",
    ],
    packages=find_packages(),
    install_requires=[],
)
