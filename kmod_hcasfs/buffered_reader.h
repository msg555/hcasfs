#ifndef _BUFFERED_READER_H
#define _BUFFERED_READER_H

#include <linux/types.h>

struct buffered_file;
struct buffered_view;

struct buffered_file *buffered_open(struct file *f);
int buffered_close(struct buffered_file *f);

struct buffered_view *buffered_view_open(struct buffered_file *bf);
void buffered_view_close(struct buffered_view *bv);

char *buffered_view_read(struct buffered_view *bv,
    char *buf, ssize_t len, loff_t *pos);

/* Same as hcasfs_read_buffered except will return ERR_PTR(-EIO) if the the full
 * length cannot be read.
 */
char *buffered_view_read_full(struct buffered_view *bv,
    char *buf, ssize_t len, loff_t *pos);

#endif