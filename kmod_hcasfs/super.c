/*
 * HCAS Filesystem - Superblock Operations
 * 
 * Handles mount, superblock management, and module init/exit
 */

#include "hcasfs.h"
#include "inode.h"

#include <linux/init.h>
#include <linux/uaccess.h>

struct hcasfs_sb_info {
	struct path hcas_data_dir;
  	char root_object_name[HCASFS_OBJECT_NAME_LEN];
	struct cred *creator_cred;
};

struct cred *hcasfs_creds(struct super_block *sb)
{
	struct hcasfs_sb_info *sb_info = sb->s_fs_info;
	return sb_info->creator_cred;
}


static int hcas_parse_hex_digit(char digit) {
  if ('0' <= digit && digit <= '9') {
    return digit - '0';
  }
  if ('a' <= digit && digit <= 'f') {
    return 10 + digit - 'a';
  }
  if ('A' <= digit && digit <= 'F') {
    return 10 + digit - 'a';
  }
  return -EINVAL;
}

static char hcas_nibble_to_hex_digit(int nibble) {
  if (10 <= nibble) {
    return 'a' + (nibble - 10);
  }
  return '0' + nibble;
}

static int hcas_parse_hex_name(char *hex_name, char object_name[HCASFS_OBJECT_NAME_LEN])
{
  if (strlen(hex_name) != HCASFS_OBJECT_NAME_LEN * 2) {
    return -EINVAL;
  }
  for (int i = 0; i < HCASFS_OBJECT_NAME_LEN; i++) {
    int front_nibble = hcas_parse_hex_digit(hex_name[2 * i + 0]);
    int back_nibble = hcas_parse_hex_digit(hex_name[2 * i + 1]);
    if (front_nibble < 0 || back_nibble < 0) {

      return -EINVAL;
    }
    object_name[i] = (front_nibble << 4) | back_nibble;
  }
  return 0;
}

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
	path_put(&sbi->hcas_data_dir);
	if (sbi->creator_cred)
		put_cred(sbi->creator_cred);
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
	.evict_inode = hcasfs_inode_evict,
};

/* Fill superblock with basic information */
int hcasfs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct hcasfs_sb_info *sbi;
	struct inode *root_inode;
	struct dentry *root_dentry;
	int ret;
	void *mount_options;

	/* Unpack mount data */
	struct hcasfs_mount_data *mount_data = data;

	mount_options = mount_data->data;

	printk(KERN_INFO "hcasfs: Filling superblock with directory handle\n");

	/* Allocate superblock private data */
	sbi = kzalloc(sizeof(struct hcasfs_sb_info), GFP_KERNEL);
	if (!sbi) {
		printk(KERN_ERR "hcasfs: Failed to allocate sb_info\n");
		return -ENOMEM;
	}

	/* Store directory file handle */
	memcpy(&sbi->hcas_data_dir, &mount_data->hcas_data_dir, sizeof(struct path));
	path_get(&sbi->hcas_data_dir);

	sbi->creator_cred = prepare_creds();
	if (!sbi->creator_cred) {
		hcasfs_free_sb_info(sbi);
		return -ENOMEM;
	}

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
	sb->s_flags = SB_RDONLY;

	/* Create root inode */
	root_inode = hcas_new_inode(sb, sbi->root_object_name);
	if (IS_ERR(root_inode)) {
		printk(KERN_ERR "hcasfs: Failed to allocate root inode\n");
		return PTR_ERR(root_inode);
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

/* path_buf should be at least HCASFS_MAX_OBJECT_PATH_LEN in size */
static void hcas_build_object_path(char *path_buf, char obj_name[HCASFS_OBJECT_NAME_LEN])
{
  path_buf[2] = '/';
  for (int i = 0; i < HCASFS_OBJECT_NAME_LEN; i++) {
    int offset = 2 * i;
    if (i) {
      offset++;
    }

    path_buf[offset + 0] = hcas_nibble_to_hex_digit((obj_name[i] >> 4) & 0xf);
    path_buf[offset + 1] = hcas_nibble_to_hex_digit(obj_name[i] & 0xf);
  }
  path_buf[1 + HCASFS_OBJECT_NAME_LEN * 2] = 0;
}

int hcas_lookup_object(struct hcasfs_sb_info *sbi, char obj_name[HCASFS_OBJECT_NAME_LEN], struct path *out)
{
    char rel_path[HCASFS_MAX_OBJECT_PATH_LEN];

    hcas_build_object_path(rel_path, obj_name);

	printk(KERN_INFO "hcas lookup %s %p %p\n", rel_path, sbi, sbi->hcas_data_dir.dentry);

	return vfs_path_lookup(sbi->hcas_data_dir.dentry, sbi->hcas_data_dir.mnt, rel_path, LOOKUP_FOLLOW, out);
}