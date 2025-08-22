#include "hcasfs.h"

struct hcasfs_dir_data {
  struct hcasfs_buffered_file *buf_f;
  loff_t f_pos;
  loff_t dir_pos;
  loff_t dir_entry_count;
};

static int hcasfs_readdir_one(struct file *file, struct dir_context *ctx) {
  char buf[256]; // NAME_MAX + 1?
  ssize_t bytes_read;
  char* data;
  struct hcasfs_dir_data *file_data = file->private_data;

  bytes_read = 96;
  data = hcasfs_read_buffered(file_data->buf_f, buf, &bytes_read);
  if (!data) {
    return bytes_read;
  }
  if (bytes_read != 96) {
    return -EIO;
  }
  
  u32 mode = get_unaligned_be32(data + 0);
  u64 parent_dep_index = get_unaligned_be64(data + 84);
  u32 file_name_len = get_unaligned_be32(data + 92);

  bytes_read = ALIGN(file_name_len, 8);
  if ((size_t)bytes_read > sizeof(buf)) {
    return -EIO;
  }
  data = hcasfs_read_buffered(file_data->buf_f, buf, &bytes_read);
  if (!data) {
    hcasfs_rewind_buffered(file_data->buf_f, 96);
    return bytes_read;
  }
  if (bytes_read != ALIGN(file_name_len, 8)) {
    return -EIO;
  }

  u64 inode_num = file->f_inode->i_ino + parent_dep_index;
  return dir_emit(ctx, buf, file_name_len, inode_num, (mode & S_IFMT) >> 12);
}

static int hcasfs_opendir(struct inode *inode, struct file *file)
{
  struct hcasfs_dir_data *file_data;
  struct hcasfs_inode_info *inode_data = inode->i_private;

  file_data = kmalloc(sizeof(*file_data), GFP_KERNEL);
  if (!file_data) {
    return -ENOMEM;
  }

  file_data->buf_f = hcas_open_object(inode->i_sb->s_fs_info, inode_data->object_name);
  if (IS_ERR(file_data->buf_f)) {
    int err = PTR_ERR(file_data->buf_f);
    kfree(file_data);
    return err;
  }

  file->private_data = file_data;
  return 0;
}

static int hcasfs_release(struct inode *inode, struct file *file)
{
  struct hcasfs_dir_data *file_data = file->private_data;
  if (file_data) {
    hcasfs_close_buffered(file_data->buf_f);
    kfree(file_data);
  }
  return 0;
}

/* Custom readdir function to list our hardcoded files */
static int hcasfs_readdir(struct file *file, struct dir_context *ctx)
{
  loff_t start_pos = ctx->pos;
  struct hcasfs_dir_data *file_data = file->private_data;

  if (!file_data) {
    return -EIO;
  }

	printk(KERN_INFO "hcasfs: Reading directory at pos %lld\n", ctx->pos);
	
	/* Emit . and .. entries */
	if (!dir_emit_dots(file, ctx))
		return 0;
	

  if (file_data->dir_pos != ctx->pos) {
    /* Need to seek our position in the directory. */

    // TODO
  }

  while (ctx->pos < file_data->dir_entry_count + 2) {
    int result = hcasfs_readdir_one(file, ctx);
    if (result < 0) {
      if (ctx->pos == start_pos) {
        return 0;
      }
      return result;
    }

    file_data->dir_pos++;
		ctx->pos++;
  }

	printk(KERN_INFO "hcasfs: Directory read complete\n");
	return 0;
}

/* Directory file operations with custom readdir */
const struct file_operations hcasfs_dir_ops = {
	.open = simple_open,
	.llseek = generic_file_llseek,
	.read = generic_read_dir,
	.iterate_shared = hcasfs_readdir,
  .open = hcasfs_opendir,
  .release = hcasfs_release,
};

