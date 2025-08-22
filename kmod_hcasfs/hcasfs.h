/*
 * HCAS Filesystem - Header File
 * 
 * Shared structures, constants, and function declarations
 */

#ifndef _HCASFS_H
#define _HCASFS_H

#include <linux/fs.h>
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include <linux/statfs.h>
#include <linux/parser.h>
#include <linux/string.h>
#include <linux/mm.h>
#include <linux/align.h>
#include <linux/unaligned.h>

/* Constants */
#define HCASFS_MODULE_NAME "hcasfs"
#define HCASFS_VERSION "0.1.0"
#define HCASFS_MAGIC 0x48434153  /* "HCAS" */

#define HCASFS_OBJECT_NAME_LEN 32
#define HCASFS_MAX_OBJECT_PATH_LEN 80

/* Mount options */
enum hcasfs_param {
	Opt_root_object,
	Opt_err
};

/* Superblock private data */
struct hcasfs_sb_info {
	struct file *hcas_dir;
  char root_object_name[HCASFS_OBJECT_NAME_LEN];
};

/* Mount data passed between mount and fill_super */
struct hcasfs_mount_data {
	struct file *hcas_dir;
	void *data;
};

/* External declarations for operation structures */
extern const struct inode_operations hcasfs_dir_inode_ops;
extern const struct file_operations hcasfs_file_ops;
extern const struct file_operations hcasfs_dir_ops;
extern const struct super_operations hcasfs_sops;

/* Function declarations from different modules */

/* super.c - superblock operations */
int hcasfs_fill_super(struct super_block *sb, void *data, int silent);
void hcasfs_put_super(struct super_block *sb);
int hcasfs_parse_options(char *options, struct hcasfs_sb_info *sbi);
void hcasfs_free_sb_info(struct hcasfs_sb_info *sbi);

/* inode.c - inode operations */
struct hcasfs_inode_info {
  char object_name[HCASFS_OBJECT_NAME_LEN];
  struct hcasfs_buffered_file *buf_f;
};

struct dentry *hcasfs_lookup(struct inode *dir, struct dentry *dentry,
			     unsigned int flags);
void hcasfs_evict_inode(struct inode *inode);

/* file.c - file operations */
int hcasfs_open(struct inode *inode, struct file *file);
ssize_t hcasfs_read(struct file *file, char __user *buf, size_t len, loff_t *ppos);

/* Test data */
extern const char hcasfs_test_content[];

int hcas_parse_hex_name(char *obj_name, char object_name[HCASFS_OBJECT_NAME_LEN]);
void hcas_build_object_path(char *path_buf, char obj_name[HCASFS_OBJECT_NAME_LEN]);

struct inode* hcas_new_inode(struct super_block *sb, char hcas_object_name[HCASFS_OBJECT_NAME_LEN]);

struct hcasfs_buffered_file *hcas_open_object(struct hcasfs_sb_info *sbi, char obj_name[HCASFS_OBJECT_NAME_LEN]);


/* Buffered file ops */

struct hcasfs_buffered_file;

struct hcasfs_buffered_file *hcasfs_open_buffered(struct file *f);
int hcasfs_close_buffered(struct hcasfs_buffered_file *f);
char *hcasfs_read_buffered(struct hcasfs_buffered_file *f,
    char *buf, ssize_t len, loff_t *pos);

#endif /* _HCASFS_H */
