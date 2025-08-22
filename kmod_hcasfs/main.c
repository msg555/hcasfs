/*
 * HCAS Filesystem - Main Module
 * 
 * Module initialization, filesystem registration, and mount handling
 */

#include "hcasfs.h"
#include <linux/init.h>

/* Forward declarations */
static struct dentry *hcasfs_mount(struct file_system_type *fs_type,
				   int flags, const char *dev_name, void *data);
static void hcasfs_kill_sb(struct super_block *sb);

/* Filesystem type structure */
static struct file_system_type hcasfs_type = {
	.owner    = THIS_MODULE,
	.name     = HCASFS_MODULE_NAME,
	.mount    = hcasfs_mount,
	.kill_sb  = hcasfs_kill_sb,
	.fs_flags = FS_REQUIRES_DEV,
};

/* Mount callback */
static struct dentry *hcasfs_mount(struct file_system_type *fs_type,
				   int flags, const char *dev_name, void *data)
{
	struct hcasfs_mount_data mount_data;
	struct file *hcas_dir;
	struct dentry *result;

	printk(KERN_INFO "hcasfs: Mounting filesystem on %s\n", dev_name ? dev_name : "none");
	
	if (!dev_name) {
		printk(KERN_ERR "hcasfs: No device (hcas_path) specified\n");
		return ERR_PTR(-EINVAL);
	}

	/* Open directory in caller's namespace context */
	hcas_dir = filp_open(dev_name, O_RDONLY, 0);
	if (IS_ERR(hcas_dir)) {
		printk(KERN_ERR "hcasfs: Cannot open directory %s: %ld\n", 
		       dev_name, PTR_ERR(hcas_dir));
		return ERR_CAST(hcas_dir);
	}

	/* Verify it's actually a directory */
	if (!S_ISDIR(file_inode(hcas_dir)->i_mode)) {
		printk(KERN_ERR "hcasfs: %s is not a directory\n", dev_name);
		filp_close(hcas_dir, NULL);
		return ERR_PTR(-ENOTDIR);
	}

	/* Package directory handle and original data for fill_super */
	mount_data.hcas_dir = hcas_dir;
	mount_data.data = data;
	
	result = mount_nodev(fs_type, flags, &mount_data, hcasfs_fill_super);
	
	/* If mount failed, clean up the directory handle */
	if (IS_ERR(result)) {
		filp_close(hcas_dir, NULL);
	}
	
	return result;
}

/* Unmount callback */
static void hcasfs_kill_sb(struct super_block *sb)
{
	printk(KERN_INFO "hcasfs: Unmounting filesystem\n");
	kill_anon_super(sb);
}

static int __init hcasfs_init(void)
{
	int ret;

	printk(KERN_INFO "hcasfs: Loading HCAS filesystem module v%s\n", HCASFS_VERSION);
	
	ret = register_filesystem(&hcasfs_type);
	if (ret) {
		printk(KERN_ERR "hcasfs: Failed to register filesystem: %d\n", ret);
		return ret;
	}

	printk(KERN_INFO "hcasfs: Filesystem registered successfully\n");
	return 0;
}

static void __exit hcasfs_exit(void)
{
	printk(KERN_INFO "hcasfs: Unloading HCAS filesystem module\n");
	unregister_filesystem(&hcasfs_type);
	printk(KERN_INFO "hcasfs: Filesystem unregistered\n");
}

module_init(hcasfs_init);
module_exit(hcasfs_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("HCAS Filesystem Team");
MODULE_DESCRIPTION("Hierarchical Content Addressable Storage Filesystem");
MODULE_VERSION(HCASFS_VERSION);