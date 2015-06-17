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
#include "qemu/timer.h"
#include "sysemu/sysemu.h"
#include "migration/colo.h"
#include "trace.h"
#include "qemu/error-report.h"
#include "qemu/sockets.h"
#include "migration/failover.h"
#include "qapi-event.h"

/*
 * checkpoint interval: unit ms
 * Note: Please change this default value to 10000 when we support hybrid mode.
 */
#define CHECKPOINT_MAX_PEROID 200

/*
 * The delay time before qemu begin the procedure of default failover treatment.
 * Unit: ms
 * Fix me: This value should be able to change by command
 * 'migrate-set-parameters'
 */
#define DEFAULT_FAILOVER_DELAY 2000

static QEMUBH *colo_bh;
/* colo buffer */
#define COLO_BUFFER_BASE_SIZE (4 * 1024 * 1024)

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

static bool colo_runstate_is_stopped(void)
{
    return runstate_check(RUN_STATE_COLO) || !runstate_is_running();
}

static void secondary_vm_do_failover(void)
{
    int old_state;
    MigrationIncomingState *mis = migration_incoming_get_current();

    migrate_set_state(&mis->state, MIGRATION_STATUS_COLO,
                      MIGRATION_STATUS_COMPLETED);

    if (!autostart) {
        error_report("\"-S\" qemu option will be ignored in secondary side");
        /* recover runstate to normal migration finish state */
        autostart = true;
    }

    old_state = failover_set_state(FAILOVER_STATUS_HANDLING,
                                   FAILOVER_STATUS_COMPLETED);
    if (old_state != FAILOVER_STATUS_HANDLING) {
        error_report("Serious error while do failover for secondary VM,"
                     "old_state: %d", old_state);
        return;
    }
    /* For Secondary VM, jump to incoming co */
    if (mis->migration_incoming_co) {
        qemu_coroutine_enter(mis->migration_incoming_co, NULL);
    }
}

static void primary_vm_do_failover(void)
{
    MigrationState *s = migrate_get_current();
    int old_state;

    if (s->state != MIGRATION_STATUS_FAILED) {
        migrate_set_state(&s->state, MIGRATION_STATUS_COLO,
                          MIGRATION_STATUS_COMPLETED);
    }
    qemu_bh_schedule(s->cleanup_bh);

    vm_start();

    old_state = failover_set_state(FAILOVER_STATUS_HANDLING,
                                   FAILOVER_STATUS_COMPLETED);
    if (old_state != FAILOVER_STATUS_HANDLING) {
        error_report("Serious error while do failover for Primary VM,"
                     "old_state: %d", old_state);
        return;
    }
}

void colo_do_failover(MigrationState *s)
{
    /* Make sure vm stopped while failover */
    if (!colo_runstate_is_stopped()) {
        vm_stop_force_state(RUN_STATE_COLO);
    }

    if (get_colo_mode() == COLO_MODE_PRIMARY) {
        primary_vm_do_failover();
    } else {
        secondary_vm_do_failover();
    }
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

static int colo_do_checkpoint_transaction(MigrationState *s,
                                          QEMUSizedBuffer *buffer)
{
    int ret;
    size_t size;
    QEMUFile *trans = NULL;

    ret = colo_ctl_put(s->to_dst_file, COLO_CMD_CHECKPOINT_REQUEST, 0);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(s->from_dst_file, COLO_CMD_CHECKPOINT_REPLY);
    if (ret < 0) {
        goto out;
    }
    /* Reset colo buffer and open it for write */
    qsb_set_length(buffer, 0);
    trans = qemu_bufopen("w", buffer);
    if (!trans) {
        error_report("Open colo buffer for write failed");
        goto out;
    }

    /* suspend and save vm state to colo buffer */
    qemu_mutex_lock_iothread();
    if (failover_request_is_active()) {
        qemu_mutex_unlock_iothread();
        ret = -1;
        goto out;
    }
    /* suspend and save vm state to colo buffer */
    vm_stop_force_state(RUN_STATE_COLO);
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("run", "stop");
    /*
     * failover request bh could be called after
     * vm_stop_force_state so we check failover_request_is_active() again.
     */
    if (failover_request_is_active()) {
        ret = -1;
        goto out;
    }

    /* Disable block migration */
    s->params.blk = 0;
    s->params.shared = 0;
    qemu_savevm_state_header(trans);
    qemu_savevm_state_begin(trans, &s->params);
    qemu_mutex_lock_iothread();
    qemu_savevm_state_complete(trans);
    qemu_mutex_unlock_iothread();

    qemu_fflush(trans);

    ret = colo_ctl_put(s->to_dst_file, COLO_CMD_VMSTATE_SEND, 0);
    if (ret < 0) {
        goto out;
    }
    /* we send the total size of the vmstate first */
    size = qsb_get_length(buffer);
    ret = colo_ctl_put(s->to_dst_file, COLO_CMD_VMSTATE_SIZE, size);
    if (ret < 0) {
        goto out;
    }

    qsb_put_buffer(s->to_dst_file, buffer, size);
    qemu_fflush(s->to_dst_file);
    ret = qemu_file_get_error(s->to_dst_file);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(s->from_dst_file, COLO_CMD_VMSTATE_RECEIVED);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(s->from_dst_file, COLO_CMD_VMSTATE_LOADED);
    if (ret < 0) {
        goto out;
    }

    ret = 0;
    /* resume master */
    qemu_mutex_lock_iothread();
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

out:
    if (trans) {
        qemu_fclose(trans);
    }

    return ret;
}

static void *colo_thread(void *opaque)
{
    MigrationState *s = opaque;
    QEMUSizedBuffer *buffer = NULL;
    int64_t current_time, checkpoint_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    int64_t error_time;
    int fd, ret = 0;

    failover_init_state();

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

    buffer = qsb_create(NULL, COLO_BUFFER_BASE_SIZE);
    if (buffer == NULL) {
        ret = -ENOMEM;
        error_report("Failed to allocate buffer!");
        goto out;
    }

    qemu_mutex_lock_iothread();
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    while (s->state == MIGRATION_STATUS_COLO) {
        if (failover_request_is_active()) {
            error_report("failover request");
            goto out;
        }

        current_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
        if (current_time - checkpoint_time < CHECKPOINT_MAX_PEROID) {
            g_usleep(100000);
            continue;
        }
        /* start a colo checkpoint */
        ret = colo_do_checkpoint_transaction(s, buffer);
        if (ret < 0) {
            goto out;
        }
        checkpoint_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    }

out:
    current_time = error_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    if (ret < 0) {
        error_report("Detect some error: %s", strerror(-ret));
        qapi_event_send_colo_exit(COLO_MODE_PRIMARY, COLO_EXIT_REASON_ERROR,
                                  true, strerror(-ret), NULL);

        /* Give users time to get involved in this verdict */
        while (current_time - error_time <= DEFAULT_FAILOVER_DELAY) {
            if (failover_request_is_active()) {
                error_report("Primary VM will take over work");
                break;
            }
            usleep(100 * 1000);
            current_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
        }

        qemu_mutex_lock_iothread();
        if (!failover_request_is_active()) {
            error_report("Primary VM will take over work in default");
            failover_request_active(NULL);
        }
        qemu_mutex_unlock_iothread();
    } else {
        qapi_event_send_colo_exit(COLO_MODE_PRIMARY, COLO_EXIT_REASON_REQUEST,
                                  false, NULL, NULL);
    }

    qsb_free(buffer);
    buffer = NULL;

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
    QEMUFile *fb = NULL;
    QEMUSizedBuffer *buffer = NULL; /* Cache incoming device state */
    int  total_size;
    int64_t error_time, current_time;
    int fd, ret = 0;

    migrate_set_state(&mis->state, MIGRATION_STATUS_ACTIVE,
                      MIGRATION_STATUS_COLO);

    failover_init_state();

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

    ret = colo_init_ram_cache();
    if (ret < 0) {
        error_report("Failed to initialize ram cache");
        goto out;
    }

    buffer = qsb_create(NULL, COLO_BUFFER_BASE_SIZE);
    if (buffer == NULL) {
        error_report("Failed to allocate colo buffer!");
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

        if (failover_request_is_active()) {
            error_report("failover request");
            goto out;
        }

        ret = colo_ctl_put(mis->to_src_file, COLO_CMD_CHECKPOINT_REPLY, 0);
        if (ret < 0) {
            goto out;
        }

        ret = colo_ctl_get(mis->from_src_file, COLO_CMD_VMSTATE_SEND);
        if (ret < 0) {
            goto out;
        }

        /* read the VM state total size first */
        total_size = colo_ctl_get(mis->from_src_file, COLO_CMD_VMSTATE_SIZE);
        if (total_size <= 0) {
            goto out;
        }

        /* read vm device state into colo buffer */
        ret = qsb_fill_buffer(buffer, mis->from_src_file, total_size);
        if (ret != total_size) {
            error_report("can't get all migration data");
            goto out;
        }

        ret = colo_ctl_put(mis->to_src_file, COLO_CMD_VMSTATE_RECEIVED, 0);
        if (ret < 0) {
            goto out;
        }

        /* open colo buffer for read */
        fb = qemu_bufopen("r", buffer);
        if (!fb) {
            error_report("can't open colo buffer for read");
            goto out;
        }

        qemu_mutex_lock_iothread();
        qemu_system_reset(VMRESET_SILENT);
        if (qemu_loadvm_state(fb) < 0) {
            error_report("COLO: loadvm failed");
            qemu_mutex_unlock_iothread();
            goto out;
        }
        qemu_mutex_unlock_iothread();

        ret = colo_ctl_put(mis->to_src_file, COLO_CMD_VMSTATE_LOADED, 0);
        if (ret < 0) {
            goto out;
        }

        qemu_fclose(fb);
        fb = NULL;
    }

out:
    current_time = error_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    if (ret < 0) {
        error_report("colo incoming thread will exit, detect error: %s",
                     strerror(-ret));
        qapi_event_send_colo_exit(COLO_MODE_SECONDARY, COLO_EXIT_REASON_ERROR,
                                  true, strerror(-ret), NULL);

        /* Give users time to get involved in this verdict */
        while (current_time - error_time <= DEFAULT_FAILOVER_DELAY) {
            if (failover_request_is_active()) {
                error_report("Secondary VM will take over work");
                break;
            }
            usleep(100 * 1000);
            current_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
        }
        /* check flag again*/
        if (!failover_request_is_active()) {
            /*
            * We assume that Primary VM is still alive according to
            * heartbeat, just kill Secondary VM
            */
            error_report("SVM is going to exit in default!");
            exit(1);
        }
    } else {
        qapi_event_send_colo_exit(COLO_MODE_SECONDARY, COLO_EXIT_REASON_REQUEST,
                                  false, NULL, NULL);
    }

    if (fb) {
        qemu_fclose(fb);
    }
    qsb_free(buffer);
    /* Here, we can ensure BH is hold the global lock, and will join colo
    * incoming thread, so here it is not necessary to lock here again,
    * or there will be a deadlock error.
    */
    colo_release_ram_cache();

    if (mis->to_src_file) {
        qemu_fclose(mis->to_src_file);
    }

    migration_incoming_exit_colo();

    return NULL;
}
