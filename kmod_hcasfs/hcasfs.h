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

#define HCASFS_MODULE_NAME "hcasfs"
#define HCASFS_VERSION "0.1.0"
#define HCASFS_MAGIC 0x48434153 /* "HCAS" */

#define HCASFS_OBJECT_NAME_LEN 32
#define HCASFS_MAX_OBJECT_PATH_LEN 80

/* Mount data passed between mount and fill_super */
struct hcasfs_mount_data {
	struct path hcas_data_dir;
	void *data; /* data pointer passed to mount */
};

int hcasfs_fill_super(struct super_block *sb, void *data, int silent);

/* File operations for regular files and directories */
extern const struct file_operations hcasfs_reg_ops;
extern const struct file_operations hcasfs_dir_ops;

/* Get the creds were used to create the mount. These should be used whenever
 * accessing backing files (assuming hcasfs' permission checks themselves look
 * good for the caller). */
const struct cred *hcasfs_creds(struct super_block *sb);

/* Lookup the passed object by name and write the backing file path to *out */
int hcasfs_lookup_object(struct super_block *sb,
			 char obj_name[HCASFS_OBJECT_NAME_LEN], struct path *out);

/* Make a new inode for the given object name. */
struct inode *hcasfs_new_inode(struct super_block *sb,
			       char hcas_object_name[HCASFS_OBJECT_NAME_LEN]);

#endif /* _HCASFS_H */
