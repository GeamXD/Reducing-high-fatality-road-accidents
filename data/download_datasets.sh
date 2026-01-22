#!/bin/bash

# Create directories for organized storage
mkdir -p data/{lookup,2025_dataset,2024_prior}

echo "Downloading UK Road Safety Datasets..."
echo "========================================"

# Download lookup file
echo "Downloading lookup guide..."
wget -O data/lookup/data-guide-2024.xlsx \
  "https://assets.publishing.service.gov.uk/media/691c6440e39a085bda43eed6/dft-road-casualty-statistics-road-safety-open-dataset-data-guide-2024.xlsx"

# Download 2025 provisional datasets
echo "Downloading 2025 provisional datasets..."
wget -O data/2025_dataset/collision-provisional-2025.csv \
  "https://data.dft.gov.uk/road-accidents-safety-data/dft-road-casualty-statistics-collision-provisional-2025.csv"

wget -O data/2025_dataset/vehicle-provisional-2025.csv \
  "https://data.dft.gov.uk/road-accidents-safety-data/dft-road-casualty-statistics-vehicle-provisional-2025.csv"

wget -O data/2025_dataset/casualty-provisional-2025.csv \
  "https://data.dft.gov.uk/road-accidents-safety-data/dft-road-casualty-statistics-casualty-provisional-2025.csv"

# Download 1979-2024 historical datasets
echo "Downloading 1979-2024 historical datasets..."
wget -O data/2024_prior/collision-1979-2024.csv \
  "https://data.dft.gov.uk/road-accidents-safety-data/dft-road-casualty-statistics-collision-1979-latest-published-year.csv"

wget -O data/2024_prior/vehicle-1979-2024.csv \
  "https://data.dft.gov.uk/road-accidents-safety-data/dft-road-casualty-statistics-vehicle-1979-latest-published-year.csv"

wget -O data/2024_prior/casualty-1979-2024.csv \
  "https://data.dft.gov.uk/road-accidents-safety-data/dft-road-casualty-statistics-casualty-1979-latest-published-year.csv"

echo "========================================"
echo "Download complete! Files saved in data/ directory"
echo "Run the Python conversion script next to convert to Parquet format"

echo "Converting files to parquest"
pip install pandas pyarrow
python3 convert_to_parquet.py