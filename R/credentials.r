# ===========================================================================
#
# credentials.r: Cloud credential configuration
#
# Copyright (C) 2026    Xiuwen Zheng
#
# This is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License Version 3 as
# published by the Free Software Foundation.
#
# gdscloud is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with gdscloud.
# If not, see <http://www.gnu.org/licenses/>.


#############################################################
# Configure AWS S3 credentials
#
gdsCloudConfigS3 <- function(aws_access_key_id=NULL,
    aws_secret_access_key=NULL, region=NULL, session_token=NULL)
{
    if (!is.null(aws_access_key_id))
        .gdscloud_env$aws_access_key_id <- aws_access_key_id
    if (!is.null(aws_secret_access_key))
        .gdscloud_env$aws_secret_access_key <- aws_secret_access_key
    if (!is.null(region))
        .gdscloud_env$aws_region <- region
    if (!is.null(session_token))
        .gdscloud_env$aws_session_token <- session_token
    invisible()
}


#############################################################
# Configure GCS credentials
#
gdsCloudConfigGCS <- function(access_token=NULL)
{
    if (!is.null(access_token))
        .gdscloud_env$gcs_access_token <- access_token
    invisible()
}


#############################################################
# Configure Azure credentials
#
gdsCloudConfigAzure <- function(account_name=NULL, account_key=NULL,
    sas_token=NULL)
{
    if (!is.null(account_name))
        .gdscloud_env$azure_account_name <- account_name
    if (!is.null(account_key))
        .gdscloud_env$azure_account_key <- account_key
    if (!is.null(sas_token))
        .gdscloud_env$azure_sas_token <- sas_token
    invisible()
}


#############################################################
# Internal: get S3 credentials (R config > env vars)
#
.get_s3_credentials <- function()
{
    list(
        access_key = .gdscloud_env$aws_access_key_id %||%
            Sys.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_key = .gdscloud_env$aws_secret_access_key %||%
            Sys.getenv("AWS_SECRET_ACCESS_KEY", ""),
        region = .gdscloud_env$aws_region %||%
            Sys.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        session_token = .gdscloud_env$aws_session_token %||%
            Sys.getenv("AWS_SESSION_TOKEN", "")
    )
}


#############################################################
# Internal: get GCS credentials
#
.get_gcs_credentials <- function()
{
    list(
        access_token = .gdscloud_env$gcs_access_token %||%
            Sys.getenv("GCS_ACCESS_TOKEN", "")
    )
}


#############################################################
# Internal: get Azure credentials
#
.get_azure_credentials <- function()
{
    list(
        account_name = .gdscloud_env$azure_account_name %||%
            Sys.getenv("AZURE_STORAGE_ACCOUNT", ""),
        account_key = .gdscloud_env$azure_account_key %||%
            Sys.getenv("AZURE_STORAGE_KEY", ""),
        sas_token = .gdscloud_env$azure_sas_token %||%
            Sys.getenv("AZURE_STORAGE_SAS_TOKEN", "")
    )
}


#############################################################
# Internal: null-coalescing operator
#
`%||%` <- function(x, y)
{
    if (is.null(x)) y else x
}
