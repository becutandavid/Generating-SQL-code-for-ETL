import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Generating_dimension_tables_from_csv", # Replace with your own username
    version="1.0",
    author="David Galevski, Jovan Velkovski",
    author_email="davidgalevski@gmail.com, velkovskijovan2@gmail.com",
    description="Given a csv of the desired dimensions, the package connects to the database and creates a Warehouse"
                " with dimension tables.",
    url="https://github.com/becutandavid/Generating-SQL-code-for-ETL",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)