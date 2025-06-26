# S3PartitionWriter

A simple utility to partition event data by date, compress to JSON GZIP, and track counts in a manifest.

## Clone the repository

```bash
git clone https://github.com/your-username/S3PartitionWriter.git
cd S3PartitionWriter
```

## Set up a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

On Windows (PowerShell):

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

## Install dependencies

```bash
pip install -r requirements.txt
```

## Run via CLI

```bash
python app.py --bucket local_s3 --prefix data
```

* `--bucket` : local directory simulating your S3 bucket
* `--prefix` : path prefix under the bucket (defaults to `data`)

