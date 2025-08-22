#include "hcasfs.h"

// TODO: Can allocate a smaller buffered based on f_size as an optimization.
struct hcasfs_buffered_file {
  struct file *f;
  loff_t f_size;
  loff_t buf_pos;
  loff_t buf_size;
  char buf[4 * PAGE_SIZE];
};

// f->f_inode->i_size

struct hcasfs_buffered_file *hcasfs_open_buffered(struct file *f) {
  struct hcasfs_buffered_file *buf_f = kmalloc(sizeof(struct hcasfs_buffered_file), GFP_KERNEL);
  if (!buf_f) {
    return NULL;
  }
  buf_f->f = f;
  buf_f->f_size = f->f_inode->i_size;
  buf_f->buf_pos = 0;
  buf_f->buf_size = 0;
  return buf_f;
}

int hcasfs_close_buffered(struct hcasfs_buffered_file *f)
{
  if (f) {
    int result = filp_close(f->f, NULL);
    if (result < 0) {
      return result; 
    }
    kfree(f);
  }
  return 0;
}

char *hcasfs_read_buffered(struct hcasfs_buffered_file *f,
    char *buf, ssize_t len, loff_t *pos)
{
  /*
  TODO: Update this!

  Read *len bytes from the buffered reader. This will always read exactly *len
  bytes unless the end of file is reached. In that case *len will be updated
  with the total number of bytes read.

  Generally this is optimized for sequential reading but can support

  If internal buffers already contain the requested bytes sequentially then this
  method will return the internal buffer starting at the requested byte
  location. Otherwise it will copy data into buf and return buf. buf must always
  be large enough to hold *len bytes of data.

  On failure this will return NULL and set *len to a negative error code. No
  internal state will be modified on failure.
  */
  ssize_t buf_pos = 0;
  ssize_t len_rem = *len;
  loff_t reset_pos = f->f_pos - (f->buf_end - f->buf_pos);
  int read_count = 0;

  while (true) {
    ssize_t bytes_copy;
    ssize_t bytes_read;

    if (f->buf_pos + len_rem <= f->buf_end) {
      f->f_pos += len_rem;
      return f->buf + f->buf_pos;
    }

    bytes_copy = f->buf_end - f->buf_pos;
    if (len_rem <= bytes_copy) {
      if (buf_pos == 0) {
        // No copy case. All data is already sequentially in buffer.
        f->buf_pos += len_rem;
        return f->buf + f->buf_pos - len_rem;
      }

      // Copy data into buffer and return
      memcpy(buf + buf_pos, f->buf + f->buf_pos, len_rem);
      f->buf_pos += len_rem;
      return buf;
    }

    if (bytes_copy > 0) {
      memcpy(buf + buf_pos, f->buf + f->buf_pos, len_rem);
      buf_pos += bytes_copy;
      len_rem -= bytes_copy;
    }

    // Read full block of data
    bytes_read = kernel_read(f->f, f->buf, sizeof(f->buf), &f->f_pos);
    if (bytes_read < 0) {
      if (read_count > 0) {
        // Reset file position to where we started and clear buffer.
        f->f_pos = reset_pos;
        f->buf_pos = 0;
        f->buf_end = 0;
      }
      *len = bytes_read;
      return NULL;
    }
    read_count++;

    f->buf_pos = 0;
    f->buf_end = bytes_read;
  }
}
