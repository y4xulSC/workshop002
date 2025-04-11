# 🎵 Music Analytics ETL Pipeline with Apache Airflow 🚀

![Airflow DAG Visualization](https://img.shields.io/badge/Powered%20by-Apache%20Airflow-017CEE?logo=apacheairflow&style=for-the-badge)
![Linux Compatible](https://img.shields.io/badge/%F0%9F%90%A7-Linux%20Compatible-orange?style=for-the-badge)
![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&style=for-the-badge)

## 🌟 Project Overview
A sophisticated ETL pipeline processing music data from Spotify, Grammy Awards, and Spotify API to generate powerful music industry insights.

**⚠️ Execution Environment Notice:**  
![Linux Required](https://img.shields.io/badge/IMPORTANT-Linux%20Environment%20Required-red?style=flat-square)  
This project requires a Linux environment for proper execution. Recommended options:
- 🐧 **Native Linux** (Ubuntu 22.04+ recommended)
- ⚙️ **WSL 2** (Windows Subsystem for Linux)
- 💻 **Linux VM** (VirtualBox/VMware with Ubuntu)

## 🛠️ Tech Stack
```mermaid
graph LR
    A[Python 3.10+] --> B[Apache Airflow 2.5+]
    B --> C[PostgreSQL 13+]
    C --> D[Pandas/Numpy]
    D --> E[Google Drive API]
    E --> F[Jupyter Notebooks]

## 📂 Project Structure
.
├── 📁 airflow/          # Airflow home
│   └── 📁 dags/         # Pipeline workflows
│       ├── 🐍 task_etl.py
│       └── 🐍 workshop_2_dag.py  # Main DAG
├── 📁 data/             # Raw datasets
│   ├── 📄 spotify_dataset.csv    # 600K+ tracks
│   └── 📄 the_grammy_awards.csv  # 1958-2019 awards
├── 📁 notebooks/        # Interactive analysis
│   ├── 📓 api.ipynb     # API exploration
│   ├── 📓 grammy.ipynb  # Awards analysis
│   └── 📓 spotify.ipynb # Music features
└── 📁 src/              # Pipeline code
    ├── 📁 extract/      # Data collectors
    ├── 📁 transform/    # Data processors
    └── 📁 load/         # Data loaders

## 🚀 Quick Start
###1. Prerequisites
# Verify Python version
python3 --version  # Should be 3.10+

# Verify system resources
free -h            # 4GB+ RAM recommended
df -h              # 10GB+ free space

###2. Installation
# Clone repository
git clone https://github.com/y4xulSC/workshop002.git
cd workshop002

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

###3. Airflow Setup
# Initialize Airflow
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

###4. Running the Pipeline
# Terminal
source start_airflow.sh

##📊 Data Insights Dashboard
pie
    title Music Genre Distribution
    "Pop" : 19.66
    "Electronic/Dance" : 8.95
    "Rock/Metal" : 11.94
    "Global Sounds" : 10.25
    "Others" : 49.2

##Award Nominations
Category	            Nominations     Percentage
Record Of The Year	        45            46.2%
Song Of The Year	        43            41.8%
Best Pop Vocal Album	    36            22.1%

Artist Statistics
bar
    title Top Artists by Followers (millions)
    x-axis: Artist
    y-axis: Followers
    bar: The Beatles : 5
    bar: Scooter : 2.5
    bar: Emerging Artists : 0.8

##🛠 Maintenance Commands
# Clear cache and temporary files
find . -type d -name "__pycache__" -exec rm -r {} +

# Restart Airflow services
pkill -f "airflow"
./start_airflow.sh

# Update dependencies
pip install --upgrade -r requirements.txt