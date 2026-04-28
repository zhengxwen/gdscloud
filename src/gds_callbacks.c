// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// gds_callbacks.c: Callback adapter bridging CloudStream to gdsfmt
//
// Copyright (C) 2026    Xiuwen Zheng
//
// This file is part of gdscloud.
// LGPL-3 License
// ===========================================================

#include "cloud_stream.h"
#include <R_GDS.h>


// =====================================================================
// Callback functions matching gdsfmt's TdCbStream* signatures
// =====================================================================

/// Read callback: user_data is a CloudStream*
ssize_t gdscloud_cb_read(void *buffer, ssize_t count, void *user_data)
{
    CloudStream *cs = (CloudStream *)user_data;
    long long result = cloud_stream_read(cs, buffer, (long long)count);
    return (ssize_t)result;
}

/// Seek callback: origin matches TdSysSeekOrg (0=begin, 1=current, 2=end)
long long gdscloud_cb_seek(long long offset, int origin, void *user_data)
{
    CloudStream *cs = (CloudStream *)user_data;
    return cloud_stream_seek(cs, offset, origin);
}

/// GetSize callback
long long gdscloud_cb_getsize(void *user_data)
{
    CloudStream *cs = (CloudStream *)user_data;
    return cloud_stream_getsize(cs);
}

/// Close callback
void gdscloud_cb_close(void *user_data)
{
    CloudStream *cs = (CloudStream *)user_data;
    cloud_stream_close(cs);
}
