from setuptools import setup, find_packages

build_variables = {
    "module_name":"JET-movie-ratings",
    "version" : "0.0.1"
}

setup(name=build_variables["module_name"],
      version=build_variables["version"],
      description="JET data engineering test",
      author="Nagella Raja Shyam",
      author_email="nagellarajashyam@gmail.com",
      url="",
      package_dir={"": "src"},
      packages=find_packages("src"),
      scripts=["scripts/download_files","scripts/movie_ratings_ingestion.py"]
      )
