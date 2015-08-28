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

#include "sysemu/sysemu.h"
#include "migration/colo.h"
#include "trace.h"
#include "qemu/error-report.h"

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

    qemu_mutex_lock_iothread();
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    /*TODO: COLO checkpoint savevm loop*/

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
    /* TODO: COLO checkpoint restore loop */

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
