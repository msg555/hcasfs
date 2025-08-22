/*
 * HCAS Filesystem - File Operations
 * 
 * Handles file operations like open, read, seek
 */

#include "hcasfs.h"
#include <linux/uaccess.h>

/* Test file content */
const char hcasfs_test_content[] = "Hello from HCAS kernel module!\nThis is a test file.\n";

/* File open operation */
int hcasfs_open(struct inode *inode, struct file *file)
{
	printk(KERN_INFO "hcasfs: Opening file (inode %lu)\n", inode->i_ino);
	return 0;
}

/* File read operation */
ssize_t hcasfs_read(struct file *file, char __user *buf, size_t len, loff_t *ppos)
{
	size_t content_len = strlen(hcasfs_test_content);
	size_t to_copy;
	
	printk(KERN_INFO "hcasfs: Reading file at offset %lld, len %zu\n", *ppos, len);
	
	/* Check if we're past the end of file */
	if (*ppos >= content_len)
		return 0;
		
	/* Calculate how much to copy */
	to_copy = min(len, content_len - (size_t)*ppos);
	
	/* Copy to user buffer */
	if (copy_to_user(buf, hcasfs_test_content + *ppos, to_copy))
		return -EFAULT;
		
	/* Update file position */
	*ppos += to_copy;
	
	printk(KERN_INFO "hcasfs: Read %zu bytes\n", to_copy);
	return to_copy;
}

/* File operations for regular files */
const struct file_operations hcasfs_file_ops = {
	.open = hcasfs_open,
	.read = hcasfs_read,
	.llseek = generic_file_llseek,
};

struct hcasfs_buffered_file *hcas_open_object(struct hcasfs_sb_info *sbi, char obj_name[HCASFS_OBJECT_NAME_LEN])
{
    char rel_path[HCASFS_MAX_OBJECT_PATH_LEN];
    struct file *f;
    struct hcasfs_buffered_file *buf_f;

    hcas_build_object_path(rel_path, obj_name);

	  printk(KERN_INFO "hcasfs: Read object path %s\n", rel_path);

    // Open relative to the hcas directory handle
    f = file_open_root(&sbi->hcas_dir->f_path, rel_path, O_RDONLY, 0);
    if (IS_ERR(f)) {
      return ERR_PTR(PTR_ERR(f));
    }

    buf_f = hcasfs_open_buffered(f);
    if (!buf_f) {
      filp_close(f, NULL);
    }
    return buf_f;
}
