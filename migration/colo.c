/*
 * COarse-grain LOck-stepping Virtual Machines for Non-stop Service (COLO)
 * (a.k.a. Fault Tolerance or Continuous Replication)
 *
 * Copyright (c) 2015 HUAWEI TECHNOLOGIES CO., LTD.
 * Copyright (c) 2015 FUJITSU LIMITED
 * Copyright (c) 2015 Intel Corporation
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or
 * later.  See the COPYING file in the top-level directory.
 */

#include <unistd.h>
#include "sysemu/sysemu.h"
#include "migration/colo.h"
#include "trace.h"
#include "qemu/error-report.h"
#include "qemu/sockets.h"

static QEMUBH *colo_bh;

bool colo_supported(void)
{
    return true;
}

bool migration_in_colo_state(void)
{
    MigrationState *s = migrate_get_current();

    return (s->state == MIGRATION_STATUS_COLO);
}

bool migration_incoming_in_colo_state(void)
{
    MigrationIncomingState *mis = migration_incoming_get_current();

    return (mis && (mis->state == MIGRATION_STATUS_COLO));
}

/* colo checkpoint control helper */
static int colo_ctl_put(QEMUFile *f, uint32_t cmd, uint64_t value)
{
    int ret = 0;

    qemu_put_be32(f, cmd);
    qemu_put_be64(f, value);
    qemu_fflush(f);

    ret = qemu_file_get_error(f);
    trace_colo_ctl_put(COLOCmd_lookup[cmd]);

    return ret;
}

static int colo_ctl_get_cmd(QEMUFile *f, uint32_t *cmd)
{
    int ret = 0;

    *cmd = qemu_get_be32(f);
    ret = qemu_file_get_error(f);
    if (ret < 0) {
        return ret;
    }
    if (*cmd >= COLO_CMD_MAX) {
        error_report("Invalid colo command, get cmd:%d", *cmd);
        return -EINVAL;
    }
    trace_colo_ctl_get(COLOCmd_lookup[*cmd]);

    return 0;
}

static int colo_ctl_get(QEMUFile *f, uint32_t require)
{
    int ret;
    uint32_t cmd;
    uint64_t value;

    ret = colo_ctl_get_cmd(f, &cmd);
    if (ret < 0) {
        return ret;
    }
    if (cmd != require) {
        error_report("Unexpect colo command, expect:%d, but get cmd:%d",
                     require, cmd);
        return -EINVAL;
    }

    value = qemu_get_be64(f);
    ret = qemu_file_get_error(f);
    if (ret < 0) {
        return ret;
    }

    return value;
}

static int colo_do_checkpoint_transaction(MigrationState *s)
{
    int ret;

    ret = colo_ctl_put(s->to_dst_file, COLO_CMD_CHECKPOINT_REQUEST, 0);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(s->from_dst_file, COLO_CMD_CHECKPOINT_REPLY);
    if (ret < 0) {
        goto out;
    }

    /* TODO: suspend and save vm state to colo buffer */

    ret = colo_ctl_put(s->to_dst_file, COLO_CMD_VMSTATE_SEND, 0);
    if (ret < 0) {
        goto out;
    }

    /* TODO: send vmstate to Secondary */

    ret = colo_ctl_get(s->from_dst_file, COLO_CMD_VMSTATE_RECEIVED);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(s->from_dst_file, COLO_CMD_VMSTATE_LOADED);
    if (ret < 0) {
        goto out;
    }

    /* TODO: resume Primary */

out:
    return ret;
}

static void *colo_thread(void *opaque)
{
    MigrationState *s = opaque;
    int fd, ret = 0;

    /* Dup the fd of to_dst_file */
    fd = dup(qemu_get_fd(s->to_dst_file));
    if (fd == -1) {
        ret = -errno;
        goto out;
    }
    s->from_dst_file = qemu_fopen_socket(fd, "rb");
    if (!s->from_dst_file) {
        ret = -EINVAL;
        error_report("Open QEMUFile failed!");
        goto out;
    }

    /*
     * Wait for Secondary finish loading vm states and enter COLO
     * restore.
     */
    ret = colo_ctl_get(s->from_dst_file, COLO_CMD_CHECKPOINT_READY);
    if (ret < 0) {
        goto out;
    }

    qemu_mutex_lock_iothread();
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    while (s->state == MIGRATION_STATUS_COLO) {
        /* start a colo checkpoint */
        ret = colo_do_checkpoint_transaction(s);
        if (ret < 0) {
            goto out;
        }
    }

out:
    if (ret < 0) {
        error_report("Detect some error: %s", strerror(-ret));
    }
    migrate_set_state(&s->state, MIGRATION_STATUS_COLO,
                      MIGRATION_STATUS_COMPLETED);

    if (s->from_dst_file) {
        qemu_fclose(s->from_dst_file);
    }

    qemu_mutex_lock_iothread();
    qemu_bh_schedule(s->cleanup_bh);
    qemu_mutex_unlock_iothread();

    return NULL;
}

static void colo_start_checkpointer(void *opaque)
{
    MigrationState *s = opaque;

    if (colo_bh) {
        qemu_bh_delete(colo_bh);
        colo_bh = NULL;
    }

    qemu_mutex_unlock_iothread();
    qemu_thread_join(&s->thread);
    qemu_mutex_lock_iothread();

    migrate_set_state(&s->state, MIGRATION_STATUS_ACTIVE,
                      MIGRATION_STATUS_COLO);

    qemu_thread_create(&s->thread, "colo", colo_thread, s,
                       QEMU_THREAD_JOINABLE);
}

void colo_init_checkpointer(MigrationState *s)
{
    colo_bh = qemu_bh_new(colo_start_checkpointer, s);
    qemu_bh_schedule(colo_bh);
}

/*
 * return:
 * 0: start a checkpoint
 * -1: some error happened, exit colo restore
 */
static int colo_wait_handle_cmd(QEMUFile *f, int *checkpoint_request)
{
    int ret;
    uint32_t cmd;
    uint64_t value;

    ret = colo_ctl_get_cmd(f, &cmd);
    if (ret < 0) {
        /* do failover ? */
        return ret;
    }
    /* Fix me: this value should be 0, which is not so good,
     * should be used for checking ?
     */
    value = qemu_get_be64(f);
    if (value != 0) {
        return -EINVAL;
    }

    switch (cmd) {
    case COLO_CMD_CHECKPOINT_REQUEST:
        *checkpoint_request = 1;
        return 0;
    default:
        return -EINVAL;
    }
}

void *colo_process_incoming_thread(void *opaque)
{
    MigrationIncomingState *mis = opaque;
    int fd, ret = 0;

    migrate_set_state(&mis->state, MIGRATION_STATUS_ACTIVE,
                      MIGRATION_STATUS_COLO);

    fd = dup(qemu_get_fd(mis->from_src_file));
    if (fd < 0) {
        ret = -errno;
        goto out;
    }
    mis->to_src_file = qemu_fopen_socket(fd, "wb");
    if (!mis->to_src_file) {
        ret = -EINVAL;
        error_report("Can't open incoming channel!");
        goto out;
    }

    ret = colo_ctl_put(mis->to_src_file, COLO_CMD_CHECKPOINT_READY, 0);
    if (ret < 0) {
        goto out;
    }

    while (mis->state == MIGRATION_STATUS_COLO) {
        int request = 0;
        int ret = colo_wait_handle_cmd(mis->from_src_file, &request);

        if (ret < 0) {
            break;
        } else {
            if (!request) {
                continue;
            }
        }

        ret = colo_ctl_put(mis->to_src_file, COLO_CMD_CHECKPOINT_REPLY, 0);
        if (ret < 0) {
            goto out;
        }

        ret = colo_ctl_get(mis->from_src_file, COLO_CMD_VMSTATE_SEND);
        if (ret < 0) {
            goto out;
        }

        /* TODO: read migration data into colo buffer */

        ret = colo_ctl_put(mis->to_src_file, COLO_CMD_VMSTATE_RECEIVED, 0);
        if (ret < 0) {
            goto out;
        }

        /* TODO: load vm state */

        ret = colo_ctl_put(mis->to_src_file, COLO_CMD_VMSTATE_LOADED, 0);
        if (ret < 0) {
            goto out;
        }
}

out:
    if (ret < 0) {
        error_report("colo incoming thread will exit, detect error: %s",
                     strerror(-ret));
    }

    if (mis->to_src_file) {
        qemu_fclose(mis->to_src_file);
    }
    migration_incoming_exit_colo();

    return NULL;
}
