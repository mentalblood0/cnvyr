import pathlib

import setuptools

if __name__ == "__main__":
    setuptools.setup(
        name="cnvyr",
        version="0.1.0",
        python_requires=">=3.12",
        keywords=["data-processing"],
        url="https://codeberg.org/mentalblood/cnvyr",
        description="Minified core for pipeline-oriented data processing applications",
        long_description=(pathlib.Path(__file__).parent / "README.md").read_text(),
        long_description_content_type="text/markdown",
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Developers",
            "Topic :: Database",
            "Topic :: Software Development :: Libraries",
            "Typing :: Typed",
            "Topic :: System :: Logging",
            "Operating System :: OS Independent",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.12",
            "License :: OSI Approved :: BSD License",
        ],
        author="mentalblood",
        author_email="neceporenkostepan@gmail.com",
        maintainer="mentalblood",
        maintainer_email="neceporenkostepan@gmail.com",
        install_requires=["psycopg"],
        packages=setuptools.find_packages(exclude=["test"]),
    )
