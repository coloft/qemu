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
#include "qmp-commands.h"
#include "qapi-types.h"
#include "net/filter.h"
#include "net/net.h"

/*
 * The delay time before qemu begin the procedure of default failover treatment.
 * Unit: ms
 * Fix me: This value should be able to change by command
 * 'migrate-set-parameters'
 */
#define DEFAULT_FAILOVER_DELAY 2000

static bool vmstate_loading;

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

    return mis && (mis->state == MIGRATION_STATUS_COLO);
}

static bool colo_runstate_is_stopped(void)
{
    return runstate_check(RUN_STATE_COLO) || !runstate_is_running();
}

static int colo_init_filter_buffers(void)
{
    Error *local_err = NULL;

    qemu_auto_add_filter_buffer(NET_FILTER_DIRECTION_RX, &local_err);
    if (local_err) {
        error_report_err(local_err);
        return -1;
    }
    filter_buffer_del_all_timers();
    return 0;
}

static void colo_cleanup_filter_buffers(void)
{
    qemu_auto_del_filter_buffer(NULL);
}

static void secondary_vm_do_failover(void)
{
    int old_state;
    MigrationIncomingState *mis = migration_incoming_get_current();

    /* Can not do failover during the process of VM's loading VMstate, Or
      * it will break the secondary VM.
      */
    if (vmstate_loading) {
        old_state = failover_set_state(FAILOVER_STATUS_HANDLING,
                                       FAILOVER_STATUS_RELAUNCH);
        if (old_state != FAILOVER_STATUS_HANDLING) {
            error_report("Unknow error while do failover for secondary VM,"
                         "old_state: %d", old_state);
        }
        return;
    }

    migrate_set_state(&mis->state, MIGRATION_STATUS_COLO,
                      MIGRATION_STATUS_COMPLETED);

    if (!autostart) {
        error_report("\"-S\" qemu option will be ignored in secondary side");
        /* recover runstate to normal migration finish state */
        autostart = true;
    }
    /* Make sure colo incoming thread not block in recv */
    if (mis->from_src_file) {
        qemu_file_shutdown(mis->from_src_file);
    }
    if (mis->to_src_file) {
        qemu_file_shutdown(mis->to_src_file);
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
    migration_end();

    if (s->from_dst_file) { /* Make sure colo thread no block in recv */
        qemu_file_shutdown(s->from_dst_file);
    }
    if (s->to_dst_file) {
        qemu_file_shutdown(s->to_dst_file);
    }
    colo_cleanup_filter_buffers();

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
    trace_colo_ctl_put(COLOCommand_lookup[cmd], value);

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
    if (*cmd >= COLO_COMMAND_MAX) {
        error_report("Invalid colo command, get cmd:%d", *cmd);
        return -EINVAL;
    }
    trace_colo_ctl_get(COLOCommand_lookup[*cmd]);

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
    int colo_shutdown, ret;
    size_t size;
    QEMUFile *trans = NULL;

    ret = colo_ctl_put(s->to_dst_file, COLO_COMMAND_CHECKPOINT_REQUEST, 0);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(s->from_dst_file, COLO_COMMAND_CHECKPOINT_REPLY);
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

    qemu_mutex_lock_iothread();
    if (failover_request_is_active()) {
        qemu_mutex_unlock_iothread();
        ret = -1;
        goto out;
    }
    colo_shutdown = colo_shutdown_requested;
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

    ret = colo_ctl_put(s->to_dst_file, COLO_COMMAND_VMSTATE_SEND, 0);
    if (ret < 0) {
        goto out;
    }

    qemu_mutex_lock_iothread();
    /* Note: device state is saved into buffer */
    ret = qemu_save_device_state(trans);
    if (ret < 0) {
        error_report("save device state error\n");
        qemu_mutex_unlock_iothread();
        goto out;
    }
    qemu_fflush(trans);
    qemu_save_ram_state(s->to_dst_file);
    qemu_mutex_unlock_iothread();

    /* we send the total size of the vmstate first */
    size = qsb_get_length(buffer);
    ret = colo_ctl_put(s->to_dst_file, COLO_COMMAND_VMSTATE_SIZE, size);
    if (ret < 0) {
        goto out;
    }

    qsb_put_buffer(s->to_dst_file, buffer, size);
    qemu_fflush(s->to_dst_file);
    ret = qemu_file_get_error(s->to_dst_file);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(s->from_dst_file, COLO_COMMAND_VMSTATE_RECEIVED);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(s->from_dst_file, COLO_COMMAND_VMSTATE_LOADED);
    if (ret < 0) {
        goto out;
    }

    filter_buffer_release_all();

    if (colo_shutdown) {
        colo_ctl_put(s->to_dst_file, COLO_COMMAND_GUEST_SHUTDOWN, 0);
        qemu_fflush(s->to_dst_file);
        colo_shutdown_requested = 0;
        qemu_system_shutdown_request_core();
        /* Fix me: Just let the colo thread exit ? */
        qemu_thread_exit(0);
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

static int colo_prepare_before_save(MigrationState *s)
{
    int ret;
    /* Disable block migration */
    s->params.blk = 0;
    s->params.shared = 0;
    qemu_savevm_state_begin(s->to_dst_file, &s->params);
    ret = qemu_file_get_error(s->to_dst_file);
    if (ret < 0) {
        error_report("save vm state begin error\n");
        return ret;
    }
    return 0;
}

static void colo_process_checkpoint(MigrationState *s)
{
    QEMUSizedBuffer *buffer = NULL;
    int64_t current_time, checkpoint_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    int64_t error_time;
    int fd, ret = 0;

    failover_init_state();

    ret = colo_init_filter_buffers();
    if (ret < 0) {
        ret = -EINVAL;
        goto out;
    }

    /* Dup the fd of to_dst_file */
    fd = dup(qemu_get_fd(s->to_dst_file));
    if (fd == -1) {
        ret = -errno;
        goto out;
    }
    s->from_dst_file = qemu_fopen_socket(fd, "rb");
    if (!s->from_dst_file) {
        ret = -EINVAL;
        error_report("Open QEMUFile from_dst_file failed");
        goto out;
    }

    ret = colo_prepare_before_save(s);
    if (ret < 0) {
        goto out;
    }

    /*
     * Wait for Secondary finish loading vm states and enter COLO
     * restore.
     */
    ret = colo_ctl_get(s->from_dst_file, COLO_COMMAND_CHECKPOINT_READY);
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

    ret = global_state_store();
    if (ret < 0) {
        goto out;
    }

    while (s->state == MIGRATION_STATUS_COLO) {
        if (failover_request_is_active()) {
            error_report("failover request");
            goto out;
        }

        current_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
        if ((current_time - checkpoint_time <
            s->parameters[MIGRATION_PARAMETER_CHECKPOINT_DELAY]) &&
            !colo_shutdown_requested) {
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
    if (ret < 0 || (!ret && !failover_request_is_active())) {
        error_report("%s: %s", __func__, strerror(-ret));
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

    /* Hope this not to be too long to loop here */
    while (failover_get_state() != FAILOVER_STATUS_COMPLETED) {
        ;
    }
    /* Must be called after failover BH is completed */
    if (s->from_dst_file) {
        qemu_fclose(s->from_dst_file);
    }
}

void migrate_start_colo_process(MigrationState *s)
{
    qemu_mutex_unlock_iothread();
    migrate_set_state(&s->state, MIGRATION_STATUS_ACTIVE,
                      MIGRATION_STATUS_COLO);
    colo_process_checkpoint(s);
    qemu_mutex_lock_iothread();
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
    case COLO_COMMAND_CHECKPOINT_REQUEST:
        *checkpoint_request = 1;
        return 0;
    case COLO_COMMAND_GUEST_SHUTDOWN:
        qemu_mutex_lock_iothread();
        vm_stop_force_state(RUN_STATE_COLO);
        qemu_system_shutdown_request_core();
        qemu_mutex_unlock_iothread();
        /* the main thread will exit and termiante the whole
        * process, do we need some cleanup?
        */
        qemu_thread_exit(0);
    default:
        return -EINVAL;
    }
}

static int colo_prepare_before_load(QEMUFile *f)
{
    int ret;

    ret = qemu_loadvm_state_begin(f);
    if (ret < 0) {
        error_report("load vm state begin error, ret=%d", ret);
        return ret;
    }
    return 0;
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
        error_report("colo incoming thread: Open QEMUFile to_src_file failed");
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

    ret = colo_prepare_before_load(mis->from_src_file);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_put(mis->to_src_file, COLO_COMMAND_CHECKPOINT_READY, 0);
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

        ret = colo_ctl_put(mis->to_src_file, COLO_COMMAND_CHECKPOINT_REPLY, 0);
        if (ret < 0) {
            goto out;
        }

        ret = colo_ctl_get(mis->from_src_file, COLO_COMMAND_VMSTATE_SEND);
        if (ret < 0) {
            goto out;
        }

        ret = qemu_load_ram_state(mis->from_src_file);
        if (ret < 0) {
            error_report("load ram state error");
            goto out;
        }
        /* read the VM state total size first */
        total_size = colo_ctl_get(mis->from_src_file,
                                  COLO_COMMAND_VMSTATE_SIZE);
        if (total_size <= 0) {
            goto out;
        }

        /* read vm device state into colo buffer */
        ret = qsb_fill_buffer(buffer, mis->from_src_file, total_size);
        if (ret != total_size) {
            error_report("can't get all migration data");
            goto out;
        }

        ret = colo_ctl_put(mis->to_src_file, COLO_COMMAND_VMSTATE_RECEIVED, 0);
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
        vmstate_loading = true;
        colo_flush_ram_cache();
        ret = qemu_load_device_state(fb);
        if (ret < 0) {
            error_report("COLO: load device state failed\n");
            vmstate_loading = false;
            qemu_mutex_unlock_iothread();
            goto out;
        }

        vmstate_loading = false;
        qemu_mutex_unlock_iothread();

        if (failover_get_state() == FAILOVER_STATUS_RELAUNCH) {
            failover_set_state(FAILOVER_STATUS_RELAUNCH, FAILOVER_STATUS_NONE);
            failover_request_active(NULL);
            goto out;
        }

        ret = colo_ctl_put(mis->to_src_file, COLO_COMMAND_VMSTATE_LOADED, 0);
        if (ret < 0) {
            goto out;
        }

        qemu_fclose(fb);
        fb = NULL;
    }

out:
    current_time = error_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    if (ret < 0 || (!ret && !failover_request_is_active())) {
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

    /* Hope this not to be too long to loop here */
    while (failover_get_state() != FAILOVER_STATUS_COMPLETED) {
        ;
    }
    /* Must be called after failover BH is completed */
    if (mis->to_src_file) {
        qemu_fclose(mis->to_src_file);
    }
    migration_incoming_exit_colo();

    return NULL;
}
