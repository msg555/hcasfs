/*
 * HCAS Filesystem - Inode Operations
 * 
 * Handles inode operations like lookup, getattr
 */

#include "hcasfs.h"


struct inode* hcas_new_inode(struct super_block *sb, char hcas_object_name[HCASFS_OBJECT_NAME_LEN])
{
  struct inode *inode;
  struct hcasfs_inode_info *info;

  info = kmalloc(sizeof(*info), GFP_KERNEL);
  if (!info) {
    return NULL;
  }
  
	inode = new_inode(sb);

  memcpy(info->object_name, hcas_object_name, HCASFS_OBJECT_NAME_LEN);
  info->buf_f = NULL;

  inode->i_private = info;

  return inode;
}

void hcasfs_evict_inode(struct inode *inode)
{
  truncate_inode_pages_final(&inode->i_data);
  clear_inode(inode);

  if (inode->i_private) {
    kfree(inode->i_private);
    inode->i_private = NULL;
  }
}

/* Inode operations for regular files (minimal - all NULL uses VFS defaults) */
static const struct inode_operations hcasfs_file_inode_ops = {
	/* All operations NULL - VFS provides defaults */
};

/* Lookup function - handles file/directory lookups */
struct dentry *hcasfs_lookup(struct inode *dir, struct dentry *dentry,
			     unsigned int flags)
{
	struct inode *inode = NULL;

	printk(KERN_INFO "hcasfs: Looking up '%s'\n", dentry->d_name.name);
	
/*
	if (strcmp(dentry->d_name.name, "hello") == 0) {
		inode = hcas_new_inode(dir->i_sb);
		if (!inode)
			return ERR_PTR(-ENOMEM);
			
		inode->i_ino = 2;
		inode->i_mode = S_IFREG | 0644;
		
		struct timespec64 now = current_time(inode);
		inode->i_atime_sec = now.tv_sec;
		inode->i_atime_nsec = now.tv_nsec;
		inode->i_mtime_sec = now.tv_sec;
		inode->i_mtime_nsec = now.tv_nsec;
		inode->i_ctime_sec = now.tv_sec;
		inode->i_ctime_nsec = now.tv_nsec;
		
		inode->i_size = strlen(hcasfs_test_content);
    inode->i_opflags = IOP_NOFOLLOW;
		inode->i_op = &hcasfs_file_inode_ops;
		inode->i_fop = &hcasfs_file_ops;
		set_nlink(inode, 1);
		
		printk(KERN_INFO "hcasfs: Created inode for 'hello' file\n");
	}
*/
	
	/* Debug before d_add */
	printk(KERN_INFO "hcasfs: About to call d_add - dentry=%p, inode=%p\n", dentry, inode);
	if (dentry) {
		printk(KERN_INFO "hcasfs: dentry->d_name.name='%s', dentry->d_sb=%p\n", 
		       dentry->d_name.name, dentry->d_sb);
	}
	if (inode) {
		printk(KERN_INFO "hcasfs: inode->i_ino=%lu, inode->i_sb=%p\n", 
		       inode->i_ino, inode->i_sb);
	}
	
	/* Associate inode with dentry (NULL inode = file not found) */
	d_add(dentry, inode);
	return NULL;
}

const struct inode_operations hcasfs_dir_inode_ops = {
	.lookup = hcasfs_lookup,
};

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

int hcas_parse_hex_name(char *hex_name, char object_name[HCASFS_OBJECT_NAME_LEN])
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

/* path_buf should be at least HCASFS_MAX_OBJECT_PATH_LEN in size */
void hcas_build_object_path(char *path_buf, char obj_name[HCASFS_OBJECT_NAME_LEN])
{
  memcpy(path_buf, "data/", 5);
  path_buf[7] = '/';

  for (int i = 0; i < HCASFS_OBJECT_NAME_LEN; i++) {
    int offset = 5 + 2 * i;
    if (i) {
      offset++;
    }

    path_buf[offset + 0] = hcas_nibble_to_hex_digit((obj_name[i] >> 4) & 0xf);
    path_buf[offset + 1] = hcas_nibble_to_hex_digit(obj_name[i] & 0xf);
  }
  path_buf[6 + HCASFS_OBJECT_NAME_LEN * 2] = 0;
}

