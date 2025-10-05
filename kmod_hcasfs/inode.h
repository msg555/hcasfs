#ifndef _INODE_H
#define _INODE_H

#include <linux/types.h>

#include "hcasfs.h"

struct hcasfs_inode_dir_info {
    int initialized;
    u32 flags;
    u32 entry_count;
    u64 tree_size;
};

struct hcasfs_inode_info {
  struct path path;
  struct buffered_file *bf;
  struct buffered_view *bv;
  union {
    struct hcasfs_inode_dir_info dir;
    int unk;
  };
};

struct buffered_file *hcasfs_inode_buffered_file(struct inode *inode);
struct hcasfs_inode_dir_info *hcasfs_inode_dir_info(struct inode *inode);

struct dentry *hcasfs_lookup(struct inode *dir, struct dentry *dentry,
			     unsigned int flags);
void hcasfs_inode_evict(struct inode *inode);

int hcasfs_inode_has_content(struct inode *inode);

extern const struct inode_operations hcasfs_dir_inode_ops;
extern const struct inode_operations hcasfs_lnk_inode_ops;
extern const struct inode_operations hcasfs_none_inode_ops;

#endif