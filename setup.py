from setuptools import setup, find_packages

setup(
    name="big_query",
    version="0.1.2",
    packages=find_packages(where="src"),
    install_requires=[
        "pandas",
        "google-cloud-bigquery",
        "google-auth"
    ],
    package_dir={"": "src"},
    python_requires=">=3.7",
    description="Paquete para la integración con Google BigQuery",
    author="David Pérez"
) 