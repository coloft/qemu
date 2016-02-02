/*
 * Replication filter
 *
 * Copyright (c) 2016 HUAWEI TECHNOLOGIES CO., LTD.
 * Copyright (c) 2016 Intel Corporation
 * Copyright (c) 2016 FUJITSU LIMITED
 *
 * Author:
 *   Wen Congyang <wency@cn.fujitsu.com>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef REPLICATION_H
#define REPLICATION_H

#include "sysemu/sysemu.h"

typedef struct ReplicationOps ReplicationOps;
typedef struct ReplicationState ReplicationState;
typedef void (*Start)(ReplicationState *rs, ReplicationMode mode, Error **errp);
typedef void (*Stop)(ReplicationState *rs, bool failover, Error **errp);
typedef void (*Checkpoint)(ReplicationState *rs, Error **errp);
typedef void (*GetError)(ReplicationState *rs, Error **errp);

struct ReplicationState {
    void *opaque;
    ReplicationOps *ops;
    QLIST_ENTRY(ReplicationState) node;
};

struct ReplicationOps{
    Start start;
    Checkpoint checkpoint;
    GetError get_error;
    Stop stop;
};


ReplicationState *replication_new(void *opaque, ReplicationOps *ops);

void replication_remove(ReplicationState *rs);

void replication_start_all(ReplicationMode mode, Error **errp);

void replication_do_checkpoint_all(Error **errp);

void replication_get_error_all(Error **errp);

void replication_stop_all(bool failover, Error **errp);

#endif /* REPLICATION_H */
