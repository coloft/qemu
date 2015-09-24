/*
 * Copyright (c) 2015 FUJITSU LIMITED
 * Author: Yang Hongyang <yanghy@cn.fujitsu.com>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or
 * later.  See the COPYING file in the top-level directory.
 */

#include "net/filter.h"
#include "net/queue.h"
#include "qemu-common.h"
#include "qemu/timer.h"
#include "qemu/iov.h"
#include "qapi/qmp/qerror.h"
#include "qapi-visit.h"
#include "qom/object.h"

#define TYPE_FILTER_BUFFER "filter-buffer"

#define FILTER_BUFFER(obj) \
    OBJECT_CHECK(FilterBufferState, (obj), TYPE_FILTER_BUFFER)

struct FilterBufferState {
    NetFilterState parent_obj;

    NetQueue *incoming_queue;
    uint32_t interval;
    QEMUTimer release_timer;
};
typedef struct FilterBufferState FilterBufferState;

static void filter_buffer_flush(NetFilterState *nf)
{
    FilterBufferState *s = FILTER_BUFFER(nf);

    if (!qemu_net_queue_flush(s->incoming_queue)) {
        /* Unable to empty the queue, purge remaining packets */
        qemu_net_queue_purge(s->incoming_queue, nf->netdev);
    }
}

static void filter_buffer_release_timer(void *opaque)
{
    NetFilterState *nf = opaque;
    FilterBufferState *s = FILTER_BUFFER(nf);
    filter_buffer_flush(nf);
    timer_mod(&s->release_timer,
              qemu_clock_get_us(QEMU_CLOCK_VIRTUAL) + s->interval);
}

/* filter APIs */
static ssize_t filter_buffer_receive_iov(NetFilterState *nf,
                                         NetClientState *sender,
                                         unsigned flags,
                                         const struct iovec *iov,
                                         int iovcnt,
                                         NetPacketSent *sent_cb)
{
    FilterBufferState *s = FILTER_BUFFER(nf);

    /*
     * we return size when buffer a packet, the sender will take it as
     * a already sent packet, so sent_cb should not be called later
     * FIXME: even if guest can't receive packet for some reasons. Filter
     * can still accept packet until its internal queue is full.
     */
    qemu_net_queue_append_iov(s->incoming_queue, sender, flags,
                              iov, iovcnt, NULL);
    return iov_size(iov, iovcnt);
}

static void filter_buffer_cleanup(NetFilterState *nf)
{
    FilterBufferState *s = FILTER_BUFFER(nf);

    if (s->interval) {
        timer_del(&s->release_timer);
    }

    /* flush packets */
    if (s->incoming_queue) {
        filter_buffer_flush(nf);
        g_free(s->incoming_queue);
    }
}

static void filter_buffer_setup(NetFilterState *nf, Error **errp)
{
    FilterBufferState *s = FILTER_BUFFER(nf);

    /*
     * this check should be dropped when there're VM FT solutions like MC
     * or COLO use this filter to release packets on demand.
     */
    if (!s->interval) {
        error_setg(errp, QERR_INVALID_PARAMETER_VALUE, "interval",
                   "a non-zero interval");
        return;
    }

    s->incoming_queue = qemu_new_net_queue(qemu_netfilter_pass_to_next, nf);
    if (s->interval) {
        timer_init_us(&s->release_timer, QEMU_CLOCK_VIRTUAL,
                      filter_buffer_release_timer, nf);
        timer_mod(&s->release_timer,
                  qemu_clock_get_us(QEMU_CLOCK_VIRTUAL) + s->interval);
    }
}

static void filter_buffer_class_init(ObjectClass *oc, void *data)
{
    NetFilterClass *nfc = NETFILTER_CLASS(oc);

    nfc->setup = filter_buffer_setup;
    nfc->cleanup = filter_buffer_cleanup;
    nfc->receive_iov = filter_buffer_receive_iov;
}

static void filter_buffer_get_interval(Object *obj, Visitor *v, void *opaque,
                                       const char *name, Error **errp)
{
    FilterBufferState *s = FILTER_BUFFER(obj);
    uint32_t value = s->interval;

    visit_type_uint32(v, &value, name, errp);
}

static void filter_buffer_set_interval(Object *obj, Visitor *v, void *opaque,
                                       const char *name, Error **errp)
{
    FilterBufferState *s = FILTER_BUFFER(obj);
    Error *local_err = NULL;
    uint32_t value;

    visit_type_uint32(v, &value, name, &local_err);
    if (local_err) {
        goto out;
    }
    if (!value) {
        error_setg(&local_err, "Property '%s.%s' doesn't take value '%"
                   PRIu32 "'", object_get_typename(obj), name, value);
        goto out;
    }
    s->interval = value;

out:
    error_propagate(errp, local_err);
}

static void filter_buffer_init(Object *obj)
{
    object_property_add(obj, "interval", "int",
                        filter_buffer_get_interval,
                        filter_buffer_set_interval, NULL, NULL, NULL);
}

static const TypeInfo filter_buffer_info = {
    .name = TYPE_FILTER_BUFFER,
    .parent = TYPE_NETFILTER,
    .class_init = filter_buffer_class_init,
    .instance_init = filter_buffer_init,
    .instance_size = sizeof(FilterBufferState),
};

static void register_types(void)
{
    type_register_static(&filter_buffer_info);
}

type_init(register_types);
