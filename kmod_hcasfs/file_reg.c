/*
 * HCAS Filesystem - File Operations
 *
 * Handles file operations like open, read, seek
 */

#include "hcasfs.h"
#include "inode.h"
#include <linux/backing-file.h>
#include <linux/uaccess.h>

struct hcasfs_file_data {
	struct file *backing_file;
};

/* File open operation */
static int hcasfs_open(struct inode *inode, struct file *file)
{
	struct file *backing_file;
	struct hcasfs_file_data *file_data;
	struct hcasfs_inode_info *inode_info = inode->i_private;

	if (!hcasfs_inode_has_content(inode))
		return 0;

	// TODO: Update this to use backing_file_open. Need to keep track of inode's
	// user path separate from real_path of the upstream backing file.
	backing_file = backing_file_open(&file->f_path, O_RDONLY,
					 &inode_info->path, current_cred());
	if (IS_ERR(file))
		return PTR_ERR(backing_file);

	file_data = kmalloc(sizeof(*file_data), GFP_KERNEL);
	if (!file_data) {
		filp_close(backing_file, 0);
		return -ENOMEM;
	}

	file_data->backing_file = backing_file;
	file->private_data = file_data;
	return 0;
}

static int hcasfs_release(struct inode *inode, struct file *file)
{
	struct hcasfs_file_data *file_data = file->private_data;

	if (file_data) {
		filp_close(file_data->backing_file, 0);
		kfree(file_data);
	}
	return 0;
}

static ssize_t hcasfs_read_iter(struct kiocb *iocb, struct iov_iter *iter)
{
	struct file *file = iocb->ki_filp;
	struct hcasfs_file_data *file_data = file->private_data;
	struct backing_file_ctx ctx = {
		.cred = hcasfs_creds(file_inode(file)->i_sb),
	};

	if (!iov_iter_count(iter))
		return 0;

	return backing_file_read_iter(file_data->backing_file, iter, iocb,
				      iocb->ki_flags, &ctx);
}

static int hcasfs_mmap(struct file *file, struct vm_area_struct *vma)
{
	struct hcasfs_file_data *file_data = file->private_data;
	struct backing_file_ctx ctx = {
		.cred = hcasfs_creds(file_inode(file)->i_sb),
	};

	/* Reject shared writable mappings */
	if ((vma->vm_flags & VM_SHARED) && (vma->vm_flags & VM_WRITE))
		return -EROFS;

	/* Private writable mappings are OK (COW) */

	return backing_file_mmap(file_data->backing_file, vma, &ctx);
}

static int hcasfs_fadvise(struct file *file, loff_t offset, loff_t len,
			  int advice)
{
	const struct cred *old_creds;
	struct hcasfs_file_data *file_data = file->private_data;
	int ret;

	old_creds = override_creds(hcasfs_creds(file_inode(file)->i_sb));
	ret = vfs_fadvise(file_data->backing_file, offset, len, advice);
	revert_creds(old_creds);

	return ret;
}

static ssize_t hcasfs_splice_read(struct file *in, loff_t *ppos,
				  struct pipe_inode_info *pipe, size_t len,
				  unsigned int flags)
{
	ssize_t ret;
	struct hcasfs_file_data *file_data = in->private_data;
	struct backing_file_ctx ctx = {
		.cred = hcasfs_creds(file_inode(in)->i_sb),
	};
	struct kiocb iocb;

	init_sync_kiocb(&iocb, in);
	iocb.ki_pos = *ppos;
	ret = backing_file_splice_read(file_data->backing_file, &iocb, pipe,
				       len, flags, &ctx);
	*ppos = iocb.ki_pos;

	return ret;
}

/* File operations for regular files */
const struct file_operations hcasfs_reg_ops = {
	.open = hcasfs_open,
	.read_iter = hcasfs_read_iter,
	.llseek = generic_file_llseek,
	.release = hcasfs_release,
	.mmap = hcasfs_mmap,
	.fadvise = hcasfs_fadvise,
	.splice_read = hcasfs_splice_read,
};
