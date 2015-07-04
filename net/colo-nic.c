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
 *
 */
#include "include/migration/migration.h"
#include "migration/colo.h"
#include "net/net.h"
#include "net/colo-nic.h"
#include "qemu/error-report.h"
#include "net/tap.h"

typedef struct nic_device {
    COLONicState *cns;
    int (*configure)(COLONicState *cns, bool up, int side, int index);
    QTAILQ_ENTRY(nic_device) next;
    bool is_up;
} nic_device;

static int launch_colo_script(COLONicState *cns, bool up, int side, int index)
{
    NetClientState *nc = container_of(cns, NetClientState, cns);
    TAPState *s = DO_UPCAST(TAPState, nc, nc);
    int i, argc = 6;
    char *argv[7], index_str[32];
    char **parg;
    Error *err = NULL;

    parg = argv;
    *parg++ = cns->script;
    *parg++ = (char *)(side == COLO_MODE_SECONDARY ? "secondary" : "primary");
    *parg++ = (char *)(up ? "install" : "uninstall");
    *parg++ = cns->nicname;
    *parg++ = cns->ifname;
    sprintf(index_str, "%d", index);
    *parg++ = index_str;
    *parg = NULL;

    for (i = 0; i < argc; i++) {
        if (!argv[i][0]) {
            error_report("Can not get colo_script argument");
            return -1;
        }
    }

    launch_script(argv, s->fd, &err);
    if (err) {
        error_report_err(err);
        return -1;
    }
    return 0;
}

/* For secondary VM, we need to cleanup its original configure
 * when go into COLO state, when exit from COLO state, we need to
 * resume its old configure
*/
static int handle_old_nic_configure(COLONicState *cns, int fd, bool cleanup)
{
    Error *err = NULL;
    char *args[3];
    char **parg;

    parg = args;
    *parg++ = cleanup ? cns->qemu_ifdown : (char *)cns->qemu_ifup;
    *parg++ = (char *)cns->ifname;
    *parg = NULL;
    launch_script(args, fd, &err);
    if (err) {
        error_report_err(err);
        return -1;
    }
    return 0;
}

QTAILQ_HEAD(, nic_device) nic_devices = QTAILQ_HEAD_INITIALIZER(nic_devices);

static int colo_nic_configure(COLONicState *cns,
            bool up, int side, int index)
{
    NetClientState *nc = container_of(cns, NetClientState, cns);
    TAPState *s = DO_UPCAST(TAPState, nc, nc);

    if (!cns && index <= 0) {
        error_report("Can not parse colo_script or forward_nic");
        return -1;
    }

    switch (side) {
    case COLO_MODE_PRIMARY:
        return launch_colo_script(cns, up, side, index);
        break;
    case COLO_MODE_SECONDARY:
        if (!cns->qemu_ifup[0] || !cns->qemu_ifdown || !cns->qemu_ifdown[0]) {
            error_report("ifup(e.g. /etc/qemu-ifup) and ifdown(e.g."
                         "/etc/qemu-ifdown)script are needed for COLO");
            return -1;
        }
        if (up) {
            if (handle_old_nic_configure(cns, s->fd, true) < 0) {
                return -1;
            }
            return launch_colo_script(cns, up, side, index);
        } else {
            if (launch_colo_script(cns, up, side, index) < 0) {
                return -1;
            }
            return handle_old_nic_configure(cns, s->fd, false);
        }
        break;
    default:
        break;
    }
    return -1;
}

void colo_add_nic_devices(COLONicState *cns)
{
    struct nic_device *nic;
    NetClientState *nc = container_of(cns, NetClientState, cns);

    if (nc->info->type == NET_CLIENT_OPTIONS_KIND_HUBPORT ||
        nc->info->type == NET_CLIENT_OPTIONS_KIND_NIC) {
        return;
    }
    QTAILQ_FOREACH(nic, &nic_devices, next) {
        NetClientState *nic_nc = container_of(nic->cns, NetClientState, cns);
        if ((nic_nc->peer && nic_nc->peer == nc) ||
            (nc->peer && nc->peer == nic_nc)) {
            return;
        }
    }

    nic = g_malloc0(sizeof(*nic));
    nic->configure = colo_nic_configure;
    nic->cns = cns;

    QTAILQ_INSERT_TAIL(&nic_devices, nic, next);
}

void colo_remove_nic_devices(COLONicState *cns)
{
    struct nic_device *nic, *next_nic;

    QTAILQ_FOREACH_SAFE(nic, &nic_devices, next, next_nic) {
        if (nic->cns == cns) {
            QTAILQ_REMOVE(&nic_devices, nic, next);
            g_free(nic);
        }
    }
}
