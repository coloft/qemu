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

#include "qemu/timer.h"
#include "sysemu/sysemu.h"
#include "migration/colo.h"
#include "trace.h"
#include "qemu/error-report.h"
#include "qemu/sockets.h"
#include "migration/failover.h"
#include "qapi-event.h"
#include "net/colo-nic.h"
#include "qmp-commands.h"

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

/* Fix me: Convert to use QAPI */
typedef enum COLOCommand {
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

    COLO_CHECKPOINT_MAX
} COLOCommand;

const char * const COLOCommand_lookup[] = {
    [COLO_CHECPOINT_READY] = "checkpoint-ready",
    [COLO_CHECKPOINT_NEW] = "checkpoint-new",
    [COLO_CHECKPOINT_SUSPENDED] = "checkpoint-suspend",
    [COLO_CHECKPOINT_SEND] = "checheckpoint-send",
    [COLO_CHECKPOINT_RECEIVED] = "checkpoint-received",
    [COLO_CHECKPOINT_LOADED] = "checkpoint-loaded",
    [COLO_CHECKPOINT_MAX] = NULL,
};

static QEMUBH *colo_bh;
static bool vmstate_loading;

int64_t colo_checkpoint_period = CHECKPOINT_MAX_PEROID;

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
    if (mis->file) { /* Make sure colo incoming thread not block in recv */
        qemu_file_shutdown(mis->file);
    }
    if (mis->colo_buffer) {
        qsb_free(mis->colo_buffer);
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

    if (s->file) { /* Make sure colo thread no block in recv */
        qemu_file_shutdown(s->file);
    }
    if (s->colo_state.buffer) {
        qsb_free(s->colo_state.buffer);
        s->colo_state.buffer = NULL;
    }
    qemu_bh_schedule(s->cleanup_bh);

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
    if (request >= COLO_CHECPOINT_READY && request < COLO_CHECKPOINT_MAX) {
        trace_colo_ctl_put(COLOCommand_lookup[request]);
    }
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

    trace_colo_ctl_get(COLOCommand_lookup[require]);
    return ret;
}

static int colo_do_checkpoint_transaction(MigrationState *s, QEMUFile *control)
{
    int ret;
    size_t size;
    QEMUFile *trans = NULL;

    ret = colo_ctl_put(s->file, COLO_CHECKPOINT_NEW);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(control, COLO_CHECKPOINT_SUSPENDED);
    if (ret < 0) {
        goto out;
    }
    /* Reset colo buffer and open it for write */
    qsb_set_length(s->colo_state.buffer, 0);
    trans = qemu_bufopen("w", s->colo_state.buffer);
    if (!trans) {
        error_report("Open colo buffer for write failed");
        goto out;
    }

    if (failover_request_is_active()) {
        ret = -1;
        goto out;
    }
    /* suspend and save vm state to colo buffer */
    qemu_mutex_lock_iothread();
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
    qemu_savevm_state_begin(trans, &s->params);
    qemu_mutex_lock_iothread();
    qemu_savevm_state_complete(trans);
    qemu_mutex_unlock_iothread();

    qemu_fflush(trans);

    ret = colo_ctl_put(s->file, COLO_CHECKPOINT_SEND);
    if (ret < 0) {
        goto out;
    }
    /* we send the total size of the vmstate first */
    size = qsb_get_length(s->colo_state.buffer);
    ret = colo_ctl_put(s->file, size);
    if (ret < 0) {
        goto out;
    }

    qsb_put_buffer(s->file, s->colo_state.buffer, size);
    qemu_fflush(s->file);
    ret = qemu_file_get_error(s->file);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(control, COLO_CHECKPOINT_RECEIVED);
    if (ret < 0) {
        goto out;
    }

    ret = colo_ctl_get(control, COLO_CHECKPOINT_LOADED);
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
    QEMUFile *colo_control = NULL;
    int64_t current_time, checkpoint_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    int i, ret;

    failover_init_state();
    if (colo_proxy_init(COLO_MODE_PRIMARY) != 0) {
        error_report("Init colo proxy error");
        goto out;
    }

    colo_control = qemu_fopen_socket(qemu_get_fd(s->file), "rb");
    if (!colo_control) {
        error_report("Open colo_control failed!");
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

    s->colo_state.buffer = qsb_create(NULL, COLO_BUFFER_BASE_SIZE);
    if (s->colo_state.buffer == NULL) {
        error_report("Failed to allocate colo buffer!");
        goto out;
    }

    qemu_mutex_lock_iothread();
    vm_start();
    qemu_mutex_unlock_iothread();
    trace_colo_vm_state_change("stop", "run");

    while (s->state == MIGRATION_STATUS_COLO) {
        int proxy_checkpoint_req;

        if (failover_request_is_active()) {
            error_report("failover request");
            goto out;
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
            goto do_checkpoint;
        }

        /*
         * No proxy checkpoint is request, wait for 100ms
         * and then check if we need checkpoint again.
         */
        current_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
        if (current_time - checkpoint_time < colo_checkpoint_period) {
            g_usleep(100000);
            continue;
        }

do_checkpoint:
        /* start a colo checkpoint */
        if (colo_do_checkpoint_transaction(s, colo_control)) {
            goto out;
        }
        checkpoint_time = qemu_clock_get_ms(QEMU_CLOCK_HOST);
    }

out:
    error_report("colo: some error happens in colo_thread");
    if (colo_control) {
        qemu_fclose(colo_control);
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

    colo_proxy_destroy(COLO_MODE_PRIMARY);

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
    uint64_t cmd;

    ret = colo_ctl_get_value(f, &cmd);
    if (ret < 0) {
        return -1;
    }

    switch (cmd) {
    case COLO_CHECKPOINT_NEW:
        *checkpoint_request = 1;
        return 0;
    default:
        return -1;
    }
}

void *colo_process_incoming_checkpoints(void *opaque)
{
    MigrationIncomingState *mis = opaque;
    QEMUFile *f = mis->file;
    int fd = qemu_get_fd(f);
    QEMUFile *ctl = NULL, *fb = NULL;
    uint64_t total_size;
    int i, ret;

    migrate_set_state(&mis->state, MIGRATION_STATUS_ACTIVE,
                      MIGRATION_STATUS_COLO);
    failover_init_state();
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

    mis->colo_buffer = qsb_create(NULL, COLO_BUFFER_BASE_SIZE);
    if (mis->colo_buffer == NULL) {
        error_report("Failed to allocate colo buffer!");
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

    while (mis->state == MIGRATION_STATUS_COLO) {
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

        ret = colo_ctl_get(f, COLO_CHECKPOINT_SEND);
        if (ret < 0) {
            goto out;
        }

        /* read the VM state total size first */
        ret = colo_ctl_get_value(f, &total_size);
        if (ret < 0) {
            goto out;
        }

        /* read vm device state into colo buffer */
        ret = qsb_fill_buffer(mis->colo_buffer, f, total_size);
        if (ret != total_size) {
            error_report("can't get all migration data");
            goto out;
        }

        ret = colo_ctl_put(ctl, COLO_CHECKPOINT_RECEIVED);
        if (ret < 0) {
            goto out;
        }

        /* open colo buffer for read */
        fb = qemu_bufopen("r", mis->colo_buffer);
        if (!fb) {
            error_report("can't open colo buffer for read");
            goto out;
        }

        qemu_mutex_lock_iothread();
        qemu_system_reset(VMRESET_SILENT);
        vmstate_loading = true;
        if (qemu_loadvm_state(fb) < 0) {
            error_report("COLO: loadvm failed");
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
    error_report("Detect some error or get a failover request");
    /*
    * Here, we raise a qmp event to the user,
    * It can help user to know what happens, and help deciding whether to
    * do failover.
    */
    qapi_event_send_colo_exit("secondary", true, "unknown", NULL);

    qemu_mutex_lock_iothread();
    release_ram_cache();
    qemu_mutex_unlock_iothread();

    if (fb) {
        qemu_fclose(fb);
    }
    if (ctl) {
        qemu_fclose(ctl);
    }

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
        exit(1);
    }

    colo_proxy_destroy(COLO_MODE_SECONDARY);
    migration_incoming_exit_colo();

    return NULL;
}
