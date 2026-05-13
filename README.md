# gdscloud: Cloud Storage Access for GDS Files

GDS (Genomic Data Structure) is a high-performance file format for storing and accessing large-scale genomic data, implemented by the [gdsfmt](https://bioconductor.org/packages/gdsfmt) package. It supports hierarchical data organization with efficient random access and data compression.

The `gdscloud` package extends `gdsfmt` to provide transparent read-only access
to GDS files stored on cloud storage services or any HTTP/HTTPS URL:

- **HTTP/HTTPS** (`http://` or `https://` URLs)
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

Once gdscloud is loaded, `openfn.gds()` from the gdsfmt package automatically recognizes cloud URLs and opens them transparently. Since gdscloud works transparently through `openfn.gds()`, packages built on gdsfmt, such as [SeqArray](https://bioconductor.org/packages/SeqArray), can open cloud-hosted files directly.

```r
library(gdscloud)

# Open a GDS file from S3 — transparent via openfn.gds()
gds <- openfn.gds("s3://gds-stat/download/hapmap/hapmap_r23a.gds")
gds
## File: s3://gds-stat/download/hapmap/hapmap_r23a.gds (86.5M)
## +    [  ] *
## |--+ description   [  ] *
## |--+ sample.id   { Str8 270 LZMA_ra(17.3%), 381B } *
## |--+ variant.id   { Int32 4098136 LZMA_ra(3.19%), 511.4K } *
## |--+ position   { Int32 4098136 LZMA_ra(48.2%), 7.5M } *
## |--+ chromosome   { Str8 4098136 LZMA_ra(0.02%), 1.7K } *
## |--+ allele   { Str8 4098136 LZMA_ra(12.8%), 2.0M } *
## |--+ genotype   [  ] *
## |  |--+ data   { Bit2 2x270x4098136 LZMA_ra(12.4%), 65.4M } *
## |  |--+ extra.index   { Int32 3x0 LZMA_ra, 18B } *
## |  \--+ extra   { Int16 0 LZMA_ra, 18B }
## |--+ phase   [  ]
## |  |--+ data   { Bit1 270x4098136 LZMA_ra(0.01%), 19.8K } *
## |  |--+ extra.index   { Int32 3x0 LZMA_ra, 18B } *
## |  \--+ extra   { Bit1 0 LZMA_ra, 18B }
## |--+ annotation   [  ]
## |  |--+ id   { Str8 4098136 LZMA_ra(27.7%), 11.1M } *
## |  |--+ qual   { Float32 4098136 LZMA_ra(0.02%), 2.5K } *
## |  |--+ filter   { Int32,factor 4098136 LZMA_ra(0.02%), 2.5K } *
## |  |--+ info   [  ]
## |  \--+ format   [  ]
## \--+ sample.annotation   [  ]
##    |--+ family   { Str8 270 LZMA_ra(17.3%), 381B } *
##    |--+ father   { Str8 270 LZMA_ra(28.2%), 261B } *
##    |--+ mother   { Str8 270 LZMA_ra(28.2%), 261B } *
##    |--+ sex   { Str8 270 LZMA_ra(27.0%), 153B } *
##    \--+ phenotype   { Int32 270 LZMA_ra(8.70%), 101B } *

read.gdsn(index.gdsn(gds, "chromosome"))
## "1", ...

read.gdsn(index.gdsn(gds, "position"))
## 45162 45257 45413 46844 72434 72515 ...

closefn.gds(gds)
```

**Note:**

* SeqArray >= v1.53.1 is recommended for full cloud support. This version allows `seqParallel()` to automatically load cloud-related packages on worker processes, so parallel operations on cloud-hosted GDS files work seamlessly.

## Authentication

### HTTP/HTTPS

For public URLs, no authentication is needed:
```r
gds <- gdsCloudOpen("https://example.com/path/to/file.gds")
closefn.gds(gds)
```

For authenticated endpoints, set a Bearer token via environment variable:
```bash
export GDSCLOUD_HTTP_TOKEN=your_token
```

Or configure in R:
```r
gdsCloudConfigHTTP(bearer_token = "your_token")

# URL-specific token
gdsCloudConfigHTTP(bearer_token = "other_token", url = "https://private.example.com/")
```

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
