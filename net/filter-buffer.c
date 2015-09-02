/*
 * Copyright (c) 2015 FUJITSU LIMITED
 * Author: Yang Hongyang <yanghy@cn.fujitsu.com>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or
 * later.  See the COPYING file in the top-level directory.
 */

#include "net/filter.h"
#include "net/queue.h"
#include "filters.h"
#include "qemu-common.h"
#include "qemu/timer.h"
#include "qemu/iov.h"
#include "qapi/qmp/qerror.h"

typedef struct FilterBufferState {
    NetFilterState nf;
    NetQueue *incoming_queue;
    uint32_t interval;
    QEMUTimer release_timer;
} FilterBufferState;

static void filter_buffer_flush(NetFilterState *nf)
{
    FilterBufferState *s = DO_UPCAST(FilterBufferState, nf, nf);
    NetQueue *queue = s->incoming_queue;
    NetPacket *packet, *next;

    QTAILQ_FOREACH_SAFE(packet, &queue->packets, entry, next) {
        QTAILQ_REMOVE(&queue->packets, packet, entry);
        queue->nq_count--;

        if (packet->sender && packet->sender->peer) {
            qemu_netfilter_pass_to_next(nf, packet);
        }

        /*
         * now that we have passed the packet to next filter (or there's
         * no receiver). If it's queued by receiver's incoming_queue, there
         * will be a copy of the packet->data, so simply free this packet
         * now.
         */
        g_free(packet);
    }
}

static void filter_buffer_release_timer(void *opaque)
{
    FilterBufferState *s = opaque;
    filter_buffer_flush(&s->nf);
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
    FilterBufferState *s = DO_UPCAST(FilterBufferState, nf, nf);
    NetQueue *queue = s->incoming_queue;

    qemu_net_queue_append_iov(queue, sender, flags, iov, iovcnt, NULL);
    return iov_size(iov, iovcnt);
}

static void filter_buffer_cleanup(NetFilterState *nf)
{
    FilterBufferState *s = DO_UPCAST(FilterBufferState, nf, nf);

    if (s->interval) {
        timer_del(&s->release_timer);
    }

    /* flush packets */
    filter_buffer_flush(nf);
    g_free(s->incoming_queue);
    return;
}

static NetFilterInfo net_filter_buffer_info = {
    .type = NET_FILTER_TYPE_BUFFER,
    .size = sizeof(FilterBufferState),
    .receive_iov = filter_buffer_receive_iov,
    .cleanup = filter_buffer_cleanup,
};

int net_init_filter_buffer(const NetFilter *netfilter, const char *name,
                           int chain, NetClientState *netdev, Error **errp)
{
    NetFilterState *nf;
    FilterBufferState *s;
    const NetFilterBufferOptions *bufferopt;

    assert(netfilter->kind == NET_FILTER_TYPE_BUFFER);
    bufferopt = netfilter->buffer;

    nf = qemu_new_net_filter(&net_filter_buffer_info, netdev, name,
                             "buffer", chain);
    s = DO_UPCAST(FilterBufferState, nf, nf);
    s->incoming_queue = qemu_new_net_queue(nf);
    s->interval = bufferopt->has_interval ? bufferopt->interval : 0;
    if (s->interval) {
        timer_init_us(&s->release_timer, QEMU_CLOCK_VIRTUAL,
                      filter_buffer_release_timer, s);
        timer_mod(&s->release_timer,
                  qemu_clock_get_us(QEMU_CLOCK_VIRTUAL) + s->interval);
    }

    return 0;
}

/* public APIs */
void filter_buffer_release_all(void)
{
    NetFilterState *nfs[MAX_QUEUE_NUM];
    int queues, i;

    queues = qemu_find_netfilters_by_model("buffer", nfs, MAX_QUEUE_NUM);

    for (i = 0; i < queues; i++) {
        filter_buffer_flush(nfs[i]);
    }
}
