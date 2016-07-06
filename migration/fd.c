/*
 * QEMU live migration via generic fd
 *
 * Copyright Red Hat, Inc. 2009-2016
 *
 * Authors:
 *  Chris Lalancette <clalance@redhat.com>
 *  Daniel P. Berrange <berrange@redhat.com>
 *
 * This work is licensed under the terms of the GNU GPL, version 2.  See
 * the COPYING file in the top-level directory.
 *
 * Contributions after 2012-01-13 are licensed under the terms of the
 * GNU GPL, version 2 or (at your option) any later version.
 */

#include "qemu/osdep.h"
#include "qapi/error.h"
#include "qemu-common.h"
#include "migration/migration.h"
#include "monitor/monitor.h"
#include "io/channel-util.h"
#include "trace.h"
#include "qemu/cutils.h"

void fd_start_outgoing_migration(MigrationState *s, const char *fdname,
                                int outfd, Error **errp)
{
    QIOChannel *ioc;
    int fd = fdname ? monitor_get_fd(cur_mon, fdname, errp) : outfd;
    if (fd == -1) {
        return;
    }

    trace_migration_fd_outgoing(fd);
    ioc = qio_channel_new_fd(fd, errp);
    if (!ioc) {
        close(fd);
        return;
    }

    migration_channel_connect(s, ioc, NULL);
    object_unref(OBJECT(ioc));
}

void file_start_outgoing_migration(MigrationState *s, const char *filename,
                                   Error **errp)
{
    int fd;

    fd = qemu_open(filename, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        error_setg_errno(errp, errno, "Failed to open file: %s", filename);
        return;
    }
    /* Fix me: just for test
    *  we shouldn't use this to identify if we are do snapshot.
    */
    s->in_snapshot = true;
    fd_start_outgoing_migration(s, NULL, fd, errp);
}


static gboolean fd_accept_incoming_migration(QIOChannel *ioc,
                                             GIOCondition condition,
                                             gpointer opaque)
{
    migration_channel_process_incoming(migrate_get_current(), ioc);
    object_unref(OBJECT(ioc));
    return FALSE; /* unregister */
}

void fd_start_incoming_migration(const char *infd, int fd, Error **errp)
{
    QIOChannel *ioc;
    long in_fd;
    int err;

    if (infd) {
        err = qemu_strtol(infd, NULL, 0, &in_fd);
        if (err < 0) {
            error_setg_errno(errp, -err, "Failed to convert string '%s'"
                            " to number", infd);
            return;
        }
        fd = (int)in_fd;
    }

    trace_migration_fd_incoming(fd);

    ioc = qio_channel_new_fd(fd, errp);
    if (!ioc) {
        close(fd);
        return;
    }

    qio_channel_add_watch(ioc,
                          G_IO_IN,
                          fd_accept_incoming_migration,
                          NULL,
                          NULL);
}

void file_start_incoming_migration(const char *filename, Error **errp)
{
    int fd;

    fd = qemu_open(filename, O_RDONLY);
    if (fd < 0) {
        error_setg_errno(errp, errno, "Failed to open file:%s", filename);
        return;
    }
    fd_start_incoming_migration(NULL, fd, NULL);
}
