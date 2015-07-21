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

#include "hw/qdev-core.h"
#include "qemu/timer.h"
#include "sysemu/sysemu.h"
#include "migration/colo.h"
#include "trace.h"
#include "qemu/error-report.h"
#include "migration/failover.h"
#include "qapi-event.h"
#include "net/colo-nic.h"
#include "qmp-commands.h"
#include "block/block_int.h"

/*
* We should not do checkpoint one after another without any time interval,
* Because this will lead continuous 'stop' status for VM.
* CHECKPOINT_MIN_PERIOD is the min time limit between two checkpoint action.
*/
#define CHECKPOINT_MIN_PERIOD 100  /* unit: ms */

/*
 * force checkpoint timer: unit ms
 * this is large because COLO checkpoint will mostly depend on
 * COLO compare module.
 */
#define CHECKPOINT_MAX_PEROID 10000

enum {
    COLO_CHECPOINT_READY = 0x46,

    /*
    * Checkpoint synchronizing points.
    *
    *                  Primary                 Secondary
    *  NEW             @
    *                                          Suspend
    *  SUSPENDED                               @
    *                  Suspend&Save state
    *  SEND            @
    *                  Send state              Receive state
    *  RECEIVED                                @
    *                  Flush network           Load state
    *  LOADED                                  @
    *                  Resume                  Resume
    *
    *                  Start Comparing
    * NOTE:
    * 1) '@' who sends the message
    * 2) Every sync-point is synchronized by two sides with only
    *    one handshake(single direction) for low-latency.
    *    If more strict synchronization is required, a opposite direction
    *    sync-point should be added.
    * 3) Since sync-points are single direction, the remote side may
    *    go forward a lot when this side just receives the sync-point.
    */
    COLO_CHECKPOINT_NEW,
    COLO_CHECKPOINT_SUSPENDED,
    COLO_CHECKPOINT_SEND,
    COLO_CHECKPOINT_RECEIVED,
    COLO_CHECKPOINT_LOADED,

    COLO_GUEST_SHUTDOWN,

    COLO_RAM_LIVE_MIGRATE,
};

static QEMUBH *colo_bh;
static bool vmstate_loading;
static Coroutine *colo;

int64_t colo_checkpoint_period = CHECKPOINT_MAX_PEROID;

/* colo buffer */
#define COLO_BUFFER_BASE_SIZE (4 * 1024 * 1024)
QEMUSizedBuffer *colo_buffer;

bool colo_supported(void)
{
    return true;
}

bool migrate_in_colo_state(void)
{
    MigrationState *s = migrate_get_current();
    return (s->state == MIGRATION_STATUS_COLO);
}

bool loadvm_in_colo_state(void)
{
    return colo != NULL;
}

void qmp_colo_set_checkpoint_period(int64_t value, Error **errp)
{
    colo_checkpoint_period = value;
}

static bool colo_runstate_is_stopped(void)
{
    return runstate_check(RUN_STATE_COLO) || !runstate_is_running();
}

/*
 * there are two way to entry this function
 * 1. From colo checkpoint incoming thread, in this case
 * we should protect it by iothread lock
 * 2. From user command, because hmp/qmp command
 * was happened in main loop, iothread lock will cause a
 * dead lock.
 */
static void secondary_vm_do_failover(void)
{
    int old_state;
    Error *local_err = NULL;

    /* Can not do failover during the process of VM's loading VMstate, Or
      * it will break the secondary VM.
      */
    if (vmstate_loading) {
        old_state = failover_set_state(FAILOVER_STATUS_HANDLING,
                                       FAILOVER_STATUS_RELAUNCH);
        if (old_state != FAILOVER_STATUS_HANDLING) {
            error_report("Unknow error while do failover for secondary VM,"
                         "old_state: %d", old_state);
            return;
        }
    }

    if (colo_proxy_failover() != 0) {
        error_report("colo proxy failed to do failover");
    }
    colo_proxy_destroy(COLO_MODE_SECONDARY);

    bdrv_stop_replication_all(true, &local_err);
    if (local_err) {
        error_report_err(local_err);
    }
    trace_colo_stop_block_replication("failover");

    /* It means that VM exit from COLO state */
    colo = NULL;

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
    if (migration_incoming_co) {
        qemu_coroutine_enter(migration_incoming_co, NULL);
    }
}

static void primary_vm_do_failover(void)
{
    MigrationState *s = migrate_get_current();
    int old_state;
    Error *local_err = NULL;

    colo_proxy_destroy(COLO_MODE_PRIMARY);

    if (s->state != MIGRATION_STATUS_FAILED) {
        migrate_set_state(s, MIGRATION_STATUS_COLO, MIGRATION_STATUS_COMPLETED);
    }

    bdrv_stop_replication_all(true, &local_err);
    if (local_err) {
        error_report_err(local_err);
    }
    trace_colo_stop_block_replication("failover");

    vm_start();

    old_state = failover_set_state(FAILOVER_STATUS_HANDLING,
                                   FAILOVER_STATUS_COMPLETED);
    if (old_state != FAILOVER_STATUS_COMPLETED) {
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

    if (get_colo_mode() == COLO_MODE_SECONDARY) {
        secondary_vm_do_failover();
    } else {
        primary_vm_do_failover();
    }
}

/* colo checkpoint control helper */
static int colo_ctl_put(QEMUFile *f, uint64_t request)
{
    int ret = 0;

    qemu_put_be64(f, request);
    qemu_fflush(f);

    ret = qemu_file_get_error(f);

    return ret;
}

static int colo_ctl_get_value(QEMUFile *f, uint64_t *value)
{
    int ret = 0;
    uint64_t temp;

    temp = qemu_get_be64(f);

    ret = qemu_file_get_error(f);
    if (ret < 0) {
        return -1;
    }

    *value = temp;
    return 0;
}

static int colo_ctl_get(QEMUFile *f, uint64_t require)
{
    int ret;
    uint64_t value;

    ret = colo_ctl_get_value(f, &value);
    if (ret < 0) {
        return ret;
    }

    if (value != require) {
        error_report("unexpected state! expected: %"PRIu64
                     ", received: %"PRIu64, require, value);
        exit(1);
    }

    return ret;
}

static int colo_do_checkpoint_transaction(MigrationState *s, QEMUFile *control)
{
    int colo_shutdown, ret;
    size_t size;
    QEMUFile *trans = NULL;
    int64_t start_time, end_time, down_time;
    Error *local_err = NULL;

    ret = colo_ctl_put(s->file, COLO_CHECKPOINT_NEW);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(control, COLO_CHECKPOINT_SUSPENDED);
    if (ret < 0) {
        goto out;
    }
    /* Reset colo buffer and open it for write */
    qsb_set_length(colo_buffer, 0);
    trans = qemu_bufopen("w", colo_buffer);
    if (!trans) {
        error_report("Open colo buffer for write failed");
        goto out;
    }

    if (failover_request_is_active()) {
        ret = -1;
        goto out;
    }

    start_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    /* suspend and save vm state to colo buffer */
    qemu_mutex_lock_iothread();
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

    ret = colo_proxy_checkpoint(COLO_MODE_PRIMARY);
    if (ret < 0) {
        goto out;
    }

    /* we call this api although this may do nothing on primary side */
    qemu_mutex_lock_iothread();
    bdrv_do_checkpoint_all(&local_err);
    qemu_mutex_unlock_iothread();
    if (local_err) {
        error_report_err(local_err);
        ret = -1;
        goto out;
    }

    ret = colo_ctl_put(s->file, COLO_CHECKPOINT_SEND);
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
    ret = qemu_save_ram_state(s->file, true);
    if (ret < 0) {
        goto out;
     }
    s->colo_state.normal_transfer_pages += ret;
    qemu_mutex_unlock_iothread();

    /* we send the total size of the vmstate first */
    size = qsb_get_length(colo_buffer);
    ret = colo_ctl_put(s->file, size);
    if (ret < 0) {
        goto out;
    }

    qsb_put_buffer(s->file, colo_buffer, size);
    qemu_fflush(s->file);
    ret = qemu_file_get_error(s->file);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(control, COLO_CHECKPOINT_RECEIVED);
    if (ret < 0) {
        goto out;
    }
    trace_colo_receive_message("COLO_CHECKPOINT_RECEIVED");

    ret = colo_ctl_get(control, COLO_CHECKPOINT_LOADED);
    if (ret < 0) {
        goto out;
    }
    trace_colo_receive_message("COLO_CHECKPOINT_LOADED");

    if (colo_shutdown) {
        qemu_mutex_lock_iothread();
        bdrv_stop_replication_all(false, NULL);
        trace_colo_stop_block_replication("shutdown");
        qemu_mutex_unlock_iothread();
        colo_ctl_put(s->file, COLO_GUEST_SHUTDOWN);
        qemu_fflush(s->file);
        colo_shutdown_requested = 0;
        qemu_system_shutdown_request_core();
        while (1) {
            ;
        }
    }

    ret = 0;

    /* resume master */
    qemu_mutex_lock_iothread();
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    end_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    down_time = end_time - start_time;
    s->colo_state.total_downtime +=  down_time;
    if (down_time > s->colo_state.max_downtime) {
        s->colo_state.max_downtime = down_time ;
    }
    if (down_time < s->colo_state.min_downtime) {
        s->colo_state.min_downtime = down_time;
    }
out:
    if (trans) {
        qemu_fclose(trans);
    }

    return ret;
}

/* should be calculated by bandwidth and max downtime ? */
#define THRESHOLD_PENDING_SIZE (10 * 1024 * 1024UL)

static int colo_need_live_migrate_ram(MigrationState *s)
{
    uint64_t pending_size;
    int64_t max_size = THRESHOLD_PENDING_SIZE;

    pending_size = qemu_savevm_state_pending(s->file, max_size);
    return (pending_size && pending_size >= max_size);
}

static int  ram_prepare_before_save(MigrationState *s)
{
    int ret;

    /* Disable block migration */
    s->params.blk = 0;
    s->params.shared = 0;
    qemu_savevm_state_begin(s->file, &s->params);
    ret = qemu_file_get_error(s->file);
    if (ret < 0) {
        error_report("save vm state begin error\n");
        return ret;
    }
    return 0;
}

static void *colo_thread(void *opaque)
{
    MigrationState *s = opaque;
    int dev_hotplug = qdev_hotplug;
    QEMUFile *colo_control = NULL;
    int64_t current_time, checkpoint_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    int i, ret;
    Error *local_err = NULL;

    if (colo_proxy_init(COLO_MODE_PRIMARY) != 0) {
        error_report("Init colo proxy error");
        goto out;
    }

    colo_control = qemu_fopen_socket(qemu_get_fd(s->file), "rb");
    if (!colo_control) {
        error_report("Open colo_control failed!");
        goto out;
    }

    qdev_hotplug = 0;

    ret = ram_prepare_before_save(s);
    if (ret < 0) {
        goto out;
    }

    /*
     * Wait for Secondary finish loading vm states and enter COLO
     * restore.
     */
    ret = colo_ctl_get(colo_control, COLO_CHECPOINT_READY);
    if (ret < 0) {
        goto out;
    }
    trace_colo_receive_message("COLO_CHECPOINT_READY");

    colo_buffer = qsb_create(NULL, COLO_BUFFER_BASE_SIZE);
    if (colo_buffer == NULL) {
        error_report("Failed to allocate colo buffer!");
        goto out;
    }

    qemu_mutex_lock_iothread();
    /* start block replication */
    bdrv_start_replication_all(REPLICATION_MODE_PRIMARY, &local_err);
    if (local_err) {
        goto out;
    }
    trace_colo_start_block_replication();
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    while (s->state == MIGRATION_STATUS_COLO) {
        int proxy_checkpoint_req;

        if (failover_request_is_active()) {
            error_report("failover request");
            goto out;
        }

        if (colo_shutdown_requested) {
            goto do_checkpoint;
        }
        /* wait for a colo checkpoint */
        proxy_checkpoint_req = colo_proxy_compare();
        if (proxy_checkpoint_req < 0) {
            goto out;
        } else if (proxy_checkpoint_req) {
            int64_t interval;

            current_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
            interval = current_time - checkpoint_time;
            if (interval < CHECKPOINT_MIN_PERIOD) {
                /* Limit the min time between two checkpoint */
                g_usleep((1000*(CHECKPOINT_MIN_PERIOD - interval)));
            }
            s->colo_state.proxy_discompare_count++;
            goto do_checkpoint;
        }

        /*
         * No proxy checkpoint is request, wait for 100ms or
         * transfer some dirty ram page,
         * and then check if we need checkpoint again.
         */
        current_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
        if (current_time - checkpoint_time < colo_checkpoint_period) {
            if (colo_need_live_migrate_ram(s)) {
                ret = colo_ctl_put(s->file, COLO_RAM_LIVE_MIGRATE);
                if (ret < 0) {
                    goto out;
                }
                ret = qemu_save_ram_state(s->file, false);
                if (ret < 0) {
                    goto out;
                }
                s->colo_state.live_transfer_pages += ret;
            } else {
                g_usleep(100000);
            }
            continue;
        } else {
            s->colo_state.periodic_checkpoint_count++;
        }

do_checkpoint:
        /* start a colo checkpoint */
        if (colo_do_checkpoint_transaction(s, colo_control)) {
            goto out;
        }
        checkpoint_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
        s->colo_state.checkpoint_count++;
    }

out:
    if (local_err) {
        error_report_err(local_err);
    } else {
        error_report("colo: some error happens in colo_thread");
    }
    qapi_event_send_colo_exit("primary", true, "unknown", NULL);;
    /* Give users time (2s) to get involved in this verdict */
    for (i = 0; i < 10; i++) {
        if (failover_request_is_active()) {
            error_report("Primary VM will take over work");
            break;
        }
        usleep(200 * 1000);
    }
    qemu_mutex_lock_iothread();
    if (!failover_request_is_active()) {
        error_report("Primary VM will take over work in default");
        failover_request_active(NULL);
    }
    qemu_mutex_unlock_iothread();

    while (failover_get_state() != FAILOVER_STATUS_COMPLETED) {
        ;
    }
    failover_set_state(FAILOVER_STATUS_COMPLETED, FAILOVER_STATUS_NONE);

    qsb_free(colo_buffer);
    colo_buffer = NULL;

    if (colo_control) {
        qemu_fclose(colo_control);
    }

    qemu_mutex_lock_iothread();
    qemu_bh_schedule(s->cleanup_bh);
    qemu_mutex_unlock_iothread();

    qdev_hotplug = dev_hotplug;

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

    migrate_set_state(s, MIGRATION_STATUS_ACTIVE, MIGRATION_STATUS_COLO);

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
    uint64_t cmd;

    ret = colo_ctl_get_value(f, &cmd);
    if (ret < 0) {
        /* do failover ? */
        return -1;
    }

    switch (cmd) {
    case COLO_CHECKPOINT_NEW:
        *checkpoint_request = 1;
        return 0;
    case COLO_GUEST_SHUTDOWN:
        qemu_mutex_lock_iothread();
        vm_stop_force_state(RUN_STATE_COLO);
        bdrv_stop_replication_all(false, NULL);
        trace_colo_stop_block_replication("shutdown");
        qemu_system_shutdown_request_core();
        qemu_mutex_unlock_iothread();
        trace_colo_receive_message("COLO_GUEST_SHUTDOWN");
        /* the main thread will exit and termiante the whole
        * process, do we need some cleanup?
        */
        for (;;) {
            ;
        }
    case COLO_RAM_LIVE_MIGRATE:
        *checkpoint_request = 0;
        ret = qemu_load_ram_state(f);
        return ret;
    default:
        return -1;
    }
}

static int ram_prepare_before_load(QEMUFile *f)
{
    int ret;

    ret = qemu_loadvm_state_begin(f);
    if (ret < 0) {
        error_report("load vm state begin error, ret=%d", ret);
        return ret;
    }
    return 0;
}

void *colo_process_incoming_checkpoints(void *opaque)
{
    struct colo_incoming *colo_in = opaque;
    QEMUFile *f = colo_in->file;
    int fd = qemu_get_fd(f);
    int dev_hotplug = qdev_hotplug;
    QEMUFile *ctl = NULL, *fb = NULL;
    int i, ret;
    uint64_t total_size;
    Error *local_err = NULL;

    qdev_hotplug = 0;

    colo = qemu_coroutine_self();
    assert(colo != NULL);

    /* configure the network */
    if (colo_proxy_init(COLO_MODE_SECONDARY) != 0) {
        error_report("Init colo proxy error\n");
        goto out;
    }

    ctl = qemu_fopen_socket(fd, "wb");
    if (!ctl) {
        error_report("Can't open incoming channel!");
        goto out;
    }

    if (create_and_init_ram_cache() < 0) {
        error_report("Failed to initialize ram cache");
        goto out;
    }

    colo_buffer = qsb_create(NULL, COLO_BUFFER_BASE_SIZE);
    if (colo_buffer == NULL) {
        error_report("Failed to allocate colo buffer!");
        goto out;
    }

    qemu_mutex_lock_iothread();
    /* start block replication */
    bdrv_start_replication_all(REPLICATION_MODE_SECONDARY, &local_err);
    if (local_err) {
        goto out;
    }
    qemu_mutex_unlock_iothread();
    trace_colo_start_block_replication();

    ret = ram_prepare_before_load(f);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_put(ctl, COLO_CHECPOINT_READY);
    if (ret < 0) {
        goto out;
    }

    qemu_mutex_lock_iothread();
    /* in COLO mode, slave is runing, so start the vm */
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    while (true) {
        int request = 0;
        int ret = colo_wait_handle_cmd(f, &request);

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

        /* suspend guest */
        qemu_mutex_lock_iothread();
        vm_stop_force_state(RUN_STATE_COLO);
        qemu_mutex_unlock_iothread();
        trace_colo_vm_state_change("run", "stop");

        ret = colo_ctl_put(ctl, COLO_CHECKPOINT_SUSPENDED);
        if (ret < 0) {
            goto out;
        }

        ret = colo_proxy_checkpoint(COLO_MODE_SECONDARY);
        if (ret < 0) {
            goto out;
        }

        ret = colo_ctl_get(f, COLO_CHECKPOINT_SEND);
        if (ret < 0) {
            goto out;
        }
        trace_colo_receive_message("COLO_CHECKPOINT_SEND");

        ret = qemu_load_ram_state(f);
        if (ret < 0) {
            error_report("load ram state error");
            goto out;
        }
        /* read the VM state total size first */
        ret = colo_ctl_get_value(f, &total_size);
        if (ret < 0) {
            goto out;
        }

        /* read vm device state into colo buffer */
        ret = qsb_fill_buffer(colo_buffer, f, total_size);
        if (ret != total_size) {
            error_report("can't get all migration data");
            goto out;
        }

        ret = colo_ctl_put(ctl, COLO_CHECKPOINT_RECEIVED);
        if (ret < 0) {
            goto out;
        }
        trace_colo_receive_message("COLO_CHECKPOINT_RECEIVED");

        /* open colo buffer for read */
        fb = qemu_bufopen("r", colo_buffer);
        if (!fb) {
            error_report("can't open colo buffer for read");
            goto out;
        }

        qemu_mutex_lock_iothread();
        qemu_system_reset(VMRESET_SILENT);
        vmstate_loading = true;
        colo_flush_ram_cache();
        if (qemu_load_device_state(fb) < 0) {
            error_report("COLO: load device state failed\n");
            vmstate_loading = false;
            qemu_mutex_unlock_iothread();
            goto out;
        }

        /* discard colo disk buffer */
        bdrv_do_checkpoint_all(&local_err);
        qemu_mutex_unlock_iothread();
        if (local_err) {
            vmstate_loading = false;
            goto out;
        }

        vmstate_loading = false;

        if (failover_get_state() == FAILOVER_STATUS_RELAUNCH) {
            failover_set_state(FAILOVER_STATUS_RELAUNCH, FAILOVER_STATUS_NONE);
            failover_request_active(NULL);
            goto out;
        }
        ret = colo_ctl_put(ctl, COLO_CHECKPOINT_LOADED);
        if (ret < 0) {
            goto out;
        }

        /* resume guest */
        qemu_mutex_lock_iothread();
        vm_start();
        qemu_mutex_unlock_iothread();
        trace_colo_vm_state_change("stop", "start");

        qemu_fclose(fb);
        fb = NULL;
    }

out:
    if (local_err) {
        error_report_err(local_err);
    } else {
        error_report("Detect some error or get a failover request");
    }
    /*
    * Here, we raise a qmp event to the user,
    * It can help user to know what happens, and help deciding whether to
    * do failover.
    */
    qapi_event_send_colo_exit("secondary", true, "unknown", NULL);;
    /* Give users time (2s) to get involved in this verdict */
    for (i = 0; i < 10; i++) {
        if (failover_request_is_active()) {
            error_report("Secondary VM will take over work");
            break;
        }
        usleep(200 * 1000);
    }
    /* check flag again*/
    if (!failover_request_is_active()) {
        /*
        * We assume that Primary VM is still alive according to heartbeat,
        * just kill Secondary VM
        */
        error_report("SVM is going to exit in default!");
        colo_proxy_destroy(COLO_MODE_SECONDARY);
        exit(1);
    } else {
        /* if we went here, means Primary VM may dead, we are doing failover */
        while (failover_get_state() != FAILOVER_STATUS_COMPLETED) {
            ;
        }
        failover_set_state(FAILOVER_STATUS_COMPLETED, FAILOVER_STATUS_NONE);
    }

    if (fb) {
        qemu_fclose(fb);
    }

    release_ram_cache();
    if (ctl) {
        qemu_fclose(ctl);
    }

    qsb_free(colo_buffer);

    loadvm_exit_colo();

    qdev_hotplug = dev_hotplug;

    return NULL;
}
