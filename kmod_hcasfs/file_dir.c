#include "hcasfs.h"
#include "inode.h"

struct hcasfs_dir_data {
  struct buffered_view *bv;
  u32 entry_count;
  loff_t f_pos;
  loff_t dir_pos;
};

static int hcasfs_readdir_one(struct file *file, struct dir_context *ctx) {
  char buf[256]; // NAME_MAX + 1?
  char* data;
  struct hcasfs_dir_data *dir_data = file->private_data;

  loff_t dir_pos = dir_data->f_pos;

  printk(KERN_INFO "hcasfs: Read one!\n");

  data = buffered_view_read_full(dir_data->bv, buf, 96, &dir_pos);
  if (IS_ERR(data)) {
    return PTR_ERR(data);
  }
  
  u32 mode = get_unaligned_be32(data + 0);
  u64 parent_dep_index = get_unaligned_be64(data + 84);
  u32 file_name_len = get_unaligned_be32(data + 92);
  if (file_name_len > sizeof(buf)) {
    return -EIO;
  }

  data = buffered_view_read_full(dir_data->bv, buf, ALIGN(file_name_len, 8), &dir_pos);
  if (IS_ERR(data)) {
    return PTR_ERR(data);
  }

  dir_data->f_pos = dir_pos;
  u64 inode_num = file->f_inode->i_ino + parent_dep_index;
  return dir_emit(ctx, data, file_name_len, inode_num, (mode & S_IFMT) >> 12);
}

static int hcasfs_opendir(struct inode *inode, struct file *file)
{
  struct hcasfs_dir_data *dir_data;
  struct buffered_file *bf;
  struct hcasfs_inode_dir_info *dir_info;

  dir_data = kmalloc(sizeof(*dir_data), GFP_KERNEL);
  if (!dir_data) {
    return -ENOMEM;
  }

  bf = hcasfs_inode_buffered_file(inode);
  if (IS_ERR(bf)) {
    kfree(dir_data);
    return PTR_ERR(bf);
  }

  dir_info = hcasfs_inode_dir_info(inode);
  if (IS_ERR(dir_info)) {
    kfree(dir_data);
    return PTR_ERR(dir_data);
  }

  dir_data->bv = buffered_view_open(bf);
  if (IS_ERR(dir_data->bv)) {  
    int err = PTR_ERR(dir_data->bv);
    kfree(dir_data);
    return err;
  }

  dir_data->dir_pos = 2;
  dir_data->entry_count = dir_info->entry_count;
  dir_data->f_pos = 16 + 8 * dir_info->entry_count;

  file->private_data = dir_data;
  return 0;
}

static int hcasfs_release(struct inode *inode, struct file *file)
{
  struct hcasfs_dir_data *dir_data = file->private_data;
  if (dir_data) {
    buffered_view_close(dir_data->bv);
    kfree(dir_data);
  }
  return 0;
}


static int hcasfs_seek_dir(struct file *file, loff_t pos)
{
  loff_t read_pos;
  char dir_index_data[4];
  char *data;
  struct hcasfs_dir_data *dir_data = file->private_data;
  struct hcasfs_inode_dir_info *inode_dir_info = hcasfs_inode_dir_info(file_inode(file));

  if(WARN_ON(pos < 0 || pos >= inode_dir_info->entry_count))
    return -EIO;

  // Find the file's offset in the dirent offset table
  read_pos = 16 + 8 * pos;
  data = buffered_view_read_full(dir_data->bv, dir_index_data, sizeof(dir_index_data), &read_pos);
  if (IS_ERR(data))
    return PTR_ERR(data);

  // Set the read offset
  dir_data->f_pos = get_unaligned_be32(data);
  return 0;
}

/* Custom readdir function to list our hardcoded files */
static int hcasfs_readdir(struct file *file, struct dir_context *ctx)
{
  loff_t start_pos = ctx->pos;
  struct hcasfs_dir_data *dir_data = file->private_data;
  struct hcasfs_inode_dir_info *inode_dir_info;

  if (!dir_data) {
    return -EIO;
  }
  
  inode_dir_info = hcasfs_inode_dir_info(file_inode(file));
  if (IS_ERR(inode_dir_info)) {
    return PTR_ERR(inode_dir_info);
  }
	
	/* Emit . and .. entries */
	if (!dir_emit_dots(file, ctx))
		return 0;
	
  /* Passed the end of the directory already */
  if (ctx->pos >= inode_dir_info->entry_count + 2)
    return 0;

  if (dir_data->dir_pos != ctx->pos) {
    /* Need to seek our position in the directory. */
    int result = hcasfs_seek_dir(file, ctx->pos - 2);
    if (result)
      return result;
  }

  // Emit as many records as can fit in the buffer.
  while (ctx->pos < inode_dir_info->entry_count + 2) {
    int result = hcasfs_readdir_one(file, ctx);
    if (result < 0) {
      if (ctx->pos == start_pos) {
        return 0;
      }
      return result;
    }

    dir_data->dir_pos++;
		ctx->pos++;
  }

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

