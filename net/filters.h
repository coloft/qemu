/*
 * Copyright (c) 2015 FUJITSU LIMITED
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or
 * later.  See the COPYING file in the top-level directory.
 */

#ifndef QEMU_NET_FILTERS_H
#define QEMU_NET_FILTERS_H

#include "net/net.h"
#include "net/filter.h"

int net_init_filter_buffer(const NetFilter *netfilter, const char *name,
                           int chain, NetClientState *netdev, Error **errp);

#endif /* QEMU_NET_FILTERS_H */
