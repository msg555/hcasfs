/*
 * HCAS Filesystem - Superblock Operations
 * 
 * Handles mount, superblock management, and module init/exit
 */

#include "hcasfs.h"
#include <linux/init.h>
#include <linux/uaccess.h>

static const match_table_t hcasfs_tokens = {
	{Opt_root_object, "root_object=%s"},
	{Opt_err, NULL}
};

/* Parse mount options */
int hcasfs_parse_options(char *options, struct hcasfs_sb_info *sbi)
{
	char *p;
	substring_t args[MAX_OPT_ARGS];
	int token;
  int root_object_found = 0;

	if (!options) {
		printk(KERN_ERR "hcasfs: Missing required option: root_object\n");
		return -EINVAL;
	}

	printk(KERN_INFO "hcasfs: Parsing options: %s\n", options);

	while ((p = strsep(&options, ",")) != NULL) {
		if (!*p)
			continue;

		token = match_token(p, hcasfs_tokens, args);
		switch (token) {
		case Opt_root_object:
			char *obj_name = match_strdup(&args[0]);
      root_object_found++;
			if (!obj_name) {
				return -ENOMEM;
			}
      if (hcas_parse_hex_name(obj_name, sbi->root_object_name)) {
		    printk(KERN_ERR "hcasfs: Failed to parse root_object name\n");
        return -EINVAL;
      }
			printk(KERN_INFO "hcasfs: root_object=%s\n", obj_name);
			break;

		default:
			printk(KERN_ERR "hcasfs: Unknown mount option: %s\n", p);
			return -EINVAL;
		}
	}

	/* Validate required options */
	if (root_object_found == 0) {
		printk(KERN_ERR "hcasfs: Missing required option: root_object\n");
		return -EINVAL;
	}
	if (root_object_found > 1) {
		printk(KERN_ERR "hcasfs: root_object can only be provided once\n");
		return -EINVAL;
	}

	return 0;
}

/* Free superblock private data */
void hcasfs_free_sb_info(struct hcasfs_sb_info *sbi)
{
  if (sbi) {
    if (sbi->hcas_dir)
      filp_close(sbi->hcas_dir, NULL);
    kfree(sbi);
  }
}

/* Put superblock - cleanup private data */
void hcasfs_put_super(struct super_block *sb)
{
	printk(KERN_INFO "hcasfs: Releasing superblock\n");
  hcasfs_free_sb_info(sb->s_fs_info);
  sb->s_fs_info = NULL;
}

/* Superblock operations */
const struct super_operations hcasfs_sops = {
	.statfs = simple_statfs,
	.put_super = hcasfs_put_super,
  .evict_inode = hcasfs_evict_inode,
};

/* Fill superblock with basic information */
int hcasfs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct hcasfs_sb_info *sbi;
	struct inode *root_inode;
	struct dentry *root_dentry;
	int ret;
	struct file *hcas_dir;
	void *mount_options;

	/* Unpack mount data */
	struct hcasfs_mount_data *mount_data = data;

	hcas_dir = mount_data->hcas_dir;
	mount_options = mount_data->data;

	printk(KERN_INFO "hcasfs: Filling superblock with directory handle\n");

	/* Allocate superblock private data */
	sbi = kzalloc(sizeof(struct hcasfs_sb_info), GFP_KERNEL);
	if (!sbi) {
		printk(KERN_ERR "hcasfs: Failed to allocate sb_info\n");
		return -ENOMEM;
	}

	/* Store directory file handle */
	sbi->hcas_dir = hcas_dir;
	/* Take an extra reference since sbi now owns it */
	get_file(hcas_dir);

	/* Parse mount options */
	ret = hcasfs_parse_options((char *)mount_options, sbi);
	if (ret) {
		printk(KERN_ERR "hcasfs: Failed to parse mount options\n");
		hcasfs_free_sb_info(sbi);
		return ret;
	}

	/* Store private data in superblock */
	sb->s_fs_info = sbi;

	/* Set superblock parameters */
	sb->s_magic = HCASFS_MAGIC;
	sb->s_op = &hcasfs_sops;
	sb->s_blocksize = PAGE_SIZE;
	sb->s_blocksize_bits = PAGE_SHIFT;

	/* Create root inode */
	root_inode = hcas_new_inode(sb, sbi->root_object_name);
	if (!root_inode) {
		printk(KERN_ERR "hcasfs: Failed to allocate root inode\n");
		return -ENOMEM;
	}

	/* Set root inode attributes */
	root_inode->i_ino = 1;
	root_inode->i_mode = S_IFDIR | 0755;

	struct timespec64 now = current_time(root_inode);
	root_inode->i_atime_sec = now.tv_sec;
	root_inode->i_atime_nsec = now.tv_nsec;
	root_inode->i_mtime_sec = now.tv_sec;
	root_inode->i_mtime_nsec = now.tv_nsec;
	root_inode->i_ctime_sec = now.tv_sec;
	root_inode->i_ctime_nsec = now.tv_nsec;
	root_inode->i_op = &hcasfs_dir_inode_ops;
	root_inode->i_fop = &hcasfs_dir_ops;
	set_nlink(root_inode, 2);

	/* Create root dentry */
	root_dentry = d_make_root(root_inode);
	if (!root_dentry) {
		printk(KERN_ERR "hcasfs: Failed to allocate root dentry\n");
		iput(root_inode);  /* Release inode reference */
		return -ENOMEM;
	}

	sb->s_root = root_dentry;
	printk(KERN_INFO "hcasfs: Superblock filled successfully\n");
	return 0;
}
