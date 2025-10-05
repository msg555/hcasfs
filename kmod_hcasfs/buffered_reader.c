#include "hcasfs.h"

#include <linux/minmax.h>

#define BUF_SIZE (4 * PAGE_SIZE)

struct buffered_file {
  struct file *f;
};

struct buffered_view {
  struct buffered_file *bf;
  struct mutex lock;
  loff_t f_size;
  loff_t buf_pos;
  loff_t buf_size;
  char buf[BUF_SIZE];
};

struct buffered_file *buffered_open(struct file *f)
{
  struct buffered_file *bf;

  bf = kmalloc(sizeof(struct buffered_file), GFP_KERNEL);
  if (!bf) {
    return NULL;
  }

  bf->f = get_file(f);
  return bf;
}

int buffered_close(struct buffered_file *bf)
{
  fput(bf->f);
  kfree(bf);
  return 0;
}

struct buffered_view *buffered_view_open(struct buffered_file *bf)
{
  struct buffered_view *bv;

  size_t struct_size = sizeof(*bv);
  loff_t f_size = bf->f->f_inode->i_size;

  // If file size is smaller than buffer allocate buffer only large enough to fit.
  if (f_size < BUF_SIZE) {
    f_size -= BUF_SIZE - f_size;
  }

  bv = kmalloc(struct_size, GFP_KERNEL);
  if (!bv) {
    return NULL;
  }
  bv->bf = bf;
  bv->f_size = bf->f->f_inode->i_size;
  bv->buf_pos = 0;
  bv->buf_size = 0;
  return bv;
}

void buffered_view_close(struct buffered_view *bv)
{
  kfree(bv);
}

static ssize_t read_block(struct buffered_view *bv, loff_t off) {
  ssize_t bytes_read = 0;
  ssize_t bytes_want = sizeof(bv->buf);
  loff_t start = off;

  if (bv->f_size - off < bytes_want) {
    bytes_want = bv->f_size - off;
  }
  if (bv->buf_pos == start) {
    if (bytes_want <= bv->buf_size) {
      return 0;
    }
    bytes_read = bv->buf_size;
  }

  // Read in a full block. Usually this show be one kernel_read call.
  while (bytes_want > 0) {
    if (!bv->bf) {
      return -EIO;
    }

    ssize_t result = kernel_read(bv->bf->f, bv->buf + bytes_read, bytes_want, &off);
    if (result < 0) {
      bv->buf_pos = 0;
      bv->buf_size = 0;
      return result;
    }
    bytes_read += result;
    bytes_want -= result;
  }

  bv->buf_pos = start;
  bv->buf_size = bytes_read;

  return 0;
}

char *buffered_view_read(struct buffered_view *bv, char *buf, ssize_t len, loff_t *pos)
{
  loff_t start = *pos;
  loff_t end = start + len;
  if (end > bv->f_size) {
    end = bv->f_size;
  }
  if (start >= end) {
    return NULL;
  }

  loff_t block_start = start / BUF_SIZE;
  loff_t block_end = (end - 1) / BUF_SIZE;

  for (loff_t i = block_start; i <= block_end; i++) {
    ssize_t result = read_block(bv, i * BUF_SIZE);
    if (result < 0) {
      return ERR_PTR(result);
    }

    loff_t f_s = max_t(loff_t, i * BUF_SIZE, start);
    loff_t f_t = min_t(loff_t, (i + 1) * BUF_SIZE, end);
    if (f_s == start && f_t == end) {
      *pos += end - start;
      return bv->buf + start - bv->buf_pos;
    }
    memcpy(buf + f_s - start, bv->buf + f_s - i * BUF_SIZE, f_t - f_s);
  }

  *pos += end - start;
  return buf;
}

char *buffered_view_read_full(struct buffered_view *bv,
    char *buf, ssize_t len, loff_t *pos)
{
  loff_t pos_cpy = *pos;
  char *result = buffered_view_read(bv, buf, len, &pos_cpy);
  if (pos_cpy - *pos != len) {
    return ERR_PTR(-EIO);
  }
  *pos = pos_cpy;
  return result;
}
