/*
 * HCAS Filesystem - Header File
 *
 * Shared structures, constants, and function declarations
 */

#ifndef _HCASFS_H
#define _HCASFS_H

#include <linux/align.h>
#include <linux/file.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/namei.h>
#include <linux/parser.h>
#include <linux/slab.h>
#include <linux/statfs.h>
#include <linux/string.h>
#include <linux/unaligned.h>

#include "buffered_reader.h"

/* Constants */
#define HCASFS_MODULE_NAME "hcasfs"
#define HCASFS_VERSION "0.1.0"
#define HCASFS_MAGIC 0x48434153 /* "HCAS" */

#define HCASFS_OBJECT_NAME_LEN 32
#define HCASFS_MAX_OBJECT_PATH_LEN 80

/* Mount options */
enum hcasfs_param { Opt_root_object, Opt_err };

/* Mount data passed between mount and fill_super */
struct hcasfs_mount_data {
	struct path hcas_data_dir;
	void *data;
};

/* External declarations for operation structures */
extern const struct file_operations hcasfs_reg_ops;
extern const struct file_operations hcasfs_dir_ops;

extern const struct super_operations hcasfs_sops;

/* Function declarations from different modules */

/* super.c - superblock operations */
struct hcasfs_sb_info;

struct cred *hcasfs_creds(struct super_block *sb);

int hcasfs_fill_super(struct super_block *sb, void *data, int silent);
void hcasfs_put_super(struct super_block *sb);
int hcasfs_parse_options(char *options, struct hcasfs_sb_info *sbi);
void hcasfs_free_sb_info(struct hcasfs_sb_info *sbi);

/* Test data */
extern const char hcasfs_test_content[];

int hcas_lookup_object(struct hcasfs_sb_info *sbi,
		       char obj_name[HCASFS_OBJECT_NAME_LEN], struct path *out);

struct inode *hcas_new_inode(struct super_block *sb,
			     char hcas_object_name[HCASFS_OBJECT_NAME_LEN]);

#endif /* _HCASFS_H */
