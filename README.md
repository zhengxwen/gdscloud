# gdscloud: Cloud Storage Access for GDS Files

GDS (Genomic Data Structure) is a high-performance file format for storing and
accessing large-scale genomic data, implemented by the
[gdsfmt](https://bioconductor.org/packages/gdsfmt) package. It supports
hierarchical data organization with efficient random access and data compression.

The `gdscloud` package extends `gdsfmt` to provide transparent read-only access
to GDS files stored on cloud storage services:

- **Amazon S3** (`s3://bucket/key`)
- **Google Cloud Storage** (`gs://bucket/key`)
- **Azure Blob Storage** (`az://container/blob`)

## Installation

```r
# Install from Bioconductor
if (!requireNamespace("BiocManager", quietly=TRUE))
    install.packages("BiocManager")
BiocManager::install("gdscloud")
```

## System Requirements

- libcurl >= 7.28.0
- gdsfmt >= 1.49.1

## Maintainer

Xiuwen Zheng

## Usage

```r
library(gdscloud)

# Open a GDS file from S3 (transparent via openfn.gds)
gds <- openfn.gds("s3://my-bucket/data/example.gds")
read.gdsn(index.gdsn(gds, "genotype"))
closefn.gds(gds)

# Or use the explicit function
gds <- gdsCloudOpen("s3://my-bucket/data/example.gds")
closefn.gds(gds)
```

## Authentication

### AWS S3
Set environment variables:
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
# Optional:
export AWS_SESSION_TOKEN=your_token
```

Or configure in R:
```r
gdsCloudConfigS3(
    aws_access_key_id = "your_key",
    aws_secret_access_key = "your_secret",
    region = "us-east-1"
)
```

### Google Cloud Storage
```bash
export GCS_ACCESS_TOKEN=your_token
```

Or:
```r
gdsCloudConfigGCS(access_token = "your_token")
```

### Azure Blob Storage
```bash
export AZURE_STORAGE_ACCOUNT=your_account
export AZURE_STORAGE_KEY=your_key
# Or use SAS token:
export AZURE_STORAGE_SAS_TOKEN=your_sas
```

Or:
```r
gdsCloudConfigAzure(account_name = "your_account", account_key = "your_key")
```

## Cache Control

```r
# Set cache size (default: 64MB)
gdsCloudCacheSize(128)

# Clear all caches
gdsCloudCacheClear()

# Show cache statistics
gdsCloudCacheInfo()
```

## Also See

- [gdsfmt](https://bioconductor.org/packages/gdsfmt): Foundation package for GDS file format I/O
- [SeqArray](https://bioconductor.org/packages/SeqArray): Data management of whole-genome sequence variant calls using GDS files
