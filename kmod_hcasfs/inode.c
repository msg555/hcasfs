/*
 * HCAS Filesystem - Inode Operations
 *
 * Handles inode operations like lookup, getattr
 */


#include "inode.h"
#include "hcasfs.h"

#include <linux/crc32.h>

struct inode *hcasfs_new_inode(struct super_block *sb,
			       char hcas_object_name[HCASFS_OBJECT_NAME_LEN])
{
	struct inode *inode;
	struct hcasfs_inode_info *info;
	int result;

	info = kzalloc(sizeof(*info), GFP_KERNEL);
	if (!info)
		return ERR_PTR(-ENOMEM);

	result = hcasfs_lookup_object(sb, hcas_object_name,
				      &info->path);
	if (result) {
		kfree(info);
		return ERR_PTR(result);
	}

	inode = new_inode(sb);
	if (!inode)
		return ERR_PTR(-ENOMEM);
	if (IS_ERR(inode))
		return inode;

	if (S_ISLNK(inode->i_mode))
		inode->i_link = NULL;
	inode->i_private = info;
	return inode;
}

void hcasfs_inode_evict(struct inode *inode)
{
	struct hcasfs_inode_info *info;

	truncate_inode_pages_final(&inode->i_data);
	clear_inode(inode);

	info = inode->i_private;
	if (info) {
		if (info->bv)
			buffered_view_close(info->bv);
		if (info->bf)
			buffered_close(info->bf);
		path_put(&info->path);
		kfree(info);
		inode->i_private = NULL;
	}
	if (S_ISLNK(inode->i_mode) && inode->i_link) {
		kfree(inode->i_link);
		inode->i_link = NULL;
	}
}

struct buffered_file *hcasfs_inode_buffered_file(struct inode *inode)
{
	struct buffered_file *result;
	struct file *file;
	struct hcasfs_inode_info *info = inode->i_private;

	if (!info->bf) {
		// TODO: Take on mount creds?
		file = dentry_open(&info->path, O_RDONLY, hcasfs_creds(inode->i_sb));
		if (IS_ERR(file))
			return ERR_PTR(PTR_ERR(file));

		result = buffered_open(file);
		fput(file);
		if (IS_ERR(result))
			return result;

		info->bf = result;
	}
	return info->bf;
}

static struct buffered_view *hcasfs_inode_buffered_view(struct inode *inode)
{
	struct hcasfs_inode_info *info = inode->i_private;
	struct buffered_file *bf;
	struct buffered_view *bv;

	if (info->bv)
		return info->bv;

	bf = hcasfs_inode_buffered_file(inode);
	if (IS_ERR(bf))
		return ERR_PTR(PTR_ERR(bf));

	bv = buffered_view_open(bf);
	if (!bv)
		return ERR_PTR(-ENOMEM);

	info->bv = bv;
	return bv;
}

struct hcasfs_inode_dir_info *hcasfs_inode_dir_info(struct inode *inode)
{
	struct hcasfs_inode_info *info = inode->i_private;
	struct hcasfs_inode_dir_info *dinfo = &info->dir;
	struct buffered_view *bv;
	char *data;
	char buf[16];
	loff_t pos;

	if (dinfo->initialized)
		return dinfo;

	bv = hcasfs_inode_buffered_view(inode);
	if (IS_ERR(bv))
		return ERR_PTR(PTR_ERR(bv));

	pos = 0;
	data = buffered_view_read_full(bv, buf, 16, &pos);
	if (IS_ERR(data))
		return ERR_PTR(PTR_ERR(data));

	dinfo->initialized = true;
	dinfo->flags = get_unaligned_be32(data + 0);
	dinfo->entry_count = get_unaligned_be32(data + 4);
	dinfo->tree_size = get_unaligned_be64(data + 8);
	return dinfo;
}

/* Inode operations for regular files (minimal - all NULL uses VFS defaults) */
static const struct inode_operations hcasfs_file_inode_ops = {
	/* All operations NULL - VFS provides defaults */
};

static const struct file_operations hcasfs_default_ops = {
	/* All operations NULL - VFS provides defaults */
};

static struct inode *_lookup_at_position(struct inode *dir,
					 struct buffered_view *bv,
					 u32 record_position,
					 struct dentry *dentry)
{
	char buf[96 + 256]; // entry size + NAME_MAX
	char *data;
	struct inode *inode;
	loff_t pos;
	u32 file_name_len;
	u64 atime, mtime, ctime;

	if (WARN_ON(dentry->d_name.len > 255))
		return NULL;

	pos = record_position;
	data = buffered_view_read(bv, buf, 96 + dentry->d_name.len, &pos);
	if (IS_ERR(data))
		return ERR_PTR(PTR_ERR(data));

	// Verify name actually matches
	file_name_len = get_unaligned_be32(data + 92);
	if (file_name_len != dentry->d_name.len)
		return NULL;
	if (strncmp(data + 96, dentry->d_name.name, dentry->d_name.len))
		return NULL;

	// TODO: May need to only read object name for nodes with objects in the
	// future.
	inode = hcasfs_new_inode(dir->i_sb, data + 52);
	if (IS_ERR(inode))
		return ERR_PTR(PTR_ERR(inode));

	inode->i_mode = get_unaligned_be32(data + 0);
	inode->i_uid.val = get_unaligned_be32(data + 4);
	inode->i_gid.val = get_unaligned_be32(data + 8);

	atime = get_unaligned_be64(data + 20);
	inode->i_atime_sec = atime / 1000000000;
	inode->i_atime_nsec = atime % 1000000000;

	mtime = get_unaligned_be64(data + 28);
	inode->i_mtime_sec = mtime / 1000000000;
	inode->i_mtime_nsec = mtime % 1000000000;

	ctime = get_unaligned_be64(data + 36);
	inode->i_ctime_sec = ctime / 1000000000;
	inode->i_ctime_nsec = ctime % 1000000000;

	inode->i_size = get_unaligned_be64(data + 44);
	inode->i_ino = dir->i_ino + get_unaligned_be64(data + 84);

	if (S_ISDIR(inode->i_mode)) {
		set_nlink(inode, get_unaligned_be64(data + 12));
		inode->i_op = &hcasfs_dir_inode_ops;
		inode->i_fop = &hcasfs_dir_ops;
	} else {
		inode->i_rdev = get_unaligned_be64(data + 12);
		set_nlink(inode, 1);
		if (S_ISREG(inode->i_mode)) {
			inode->i_op = &hcasfs_none_inode_ops;
			inode->i_fop = &hcasfs_reg_ops;
		} else {
			inode->i_op = &hcasfs_lnk_inode_ops;
			inode->i_fop = &hcasfs_default_ops;
		}
	}

	return inode;
}

/* Lookup function - handles file/directory lookups */
struct dentry *hcasfs_lookup(struct inode *dir, struct dentry *dentry,
			     unsigned int flags)
{
	struct buffered_view *bv;
	struct hcasfs_inode_dir_info *dir_info;
	struct inode *inode = NULL;
	char dir_index_data[8];

	bv = hcasfs_inode_buffered_view(dir);
	if (IS_ERR(bv))
		return ERR_PTR(PTR_ERR(bv));

	dir_info = hcasfs_inode_dir_info(dir);
	if (IS_ERR(dir_info))
		return ERR_PTR(PTR_ERR(dir_info));

	u32 crc = ~crc32_le(~0, dentry->d_name.name, dentry->d_name.len);

	u32 lo = 0;
	u32 hi = dir_info->entry_count;
	u32 lo_crc = 0x00000000;
	u32 hi_crc = 0xFFFFFFFF;
	u32 ind = 0;
	u32 record_position = 0;

	while (lo < hi) {
		ind = lo + (hi - lo) / 2;

		loff_t pos = 16 + 8 * ind;
		char *data = buffered_view_read_full(
			bv, dir_index_data, sizeof(dir_index_data), &pos);

		u32 record_crc = get_unaligned_be32(data + 4);

		if (record_crc < crc) {
			lo = ind + 1;
			lo_crc = record_crc;
		} else if (record_crc > crc) {
			hi = ind;
			hi_crc = record_crc;
		} else {
			record_position = get_unaligned_be32(data + 0);
			break;
		}
	}

	if (lo == hi) {
		/* Associate inode with dentry (NULL inode = file not found) */
		d_add(dentry, inode);
		return NULL;
	}

	u32 ind_orig = ind;
	u32 iter_dir = 0;

	for (ind = ind_orig;; ind--) {
		if (iter_dir == 0) {
			iter_dir = -1;
		} else {
			if (iter_dir == -1 && ind == 0) {
				iter_dir = 1;
				continue;
			}
			if (iter_dir == 1 && ind == dir_info->entry_count)
				break;
			ind += iter_dir;

			loff_t pos = 16 + 8 * ind;
			char *data = buffered_view_read_full(
				bv, dir_index_data, sizeof(dir_index_data),
				&pos);
			if (get_unaligned_be32(data + 4) != crc) {
				if (iter_dir == -1)
					iter_dir = 1;
				else
					break;
			}
			record_position = get_unaligned_be32(data + 0);
		}

		inode = _lookup_at_position(dir, bv, record_position, dentry);
		if (IS_ERR(inode))
			return ERR_PTR(PTR_ERR(inode));
		if (inode != NULL)
			break;
	}

	/* Associate inode with dentry (NULL inode = file not found) */
	d_add(dentry, inode);
	return NULL;
}

static const char *hcasfs_get_link(struct dentry *dentry, struct inode *inode,
				   struct delayed_call *done)
{
	struct buffered_view *bv;
	char *link_data;
	char *read_data;
	loff_t pos;

	if (WARN_ON(!S_ISLNK(inode->i_mode)))
		return ERR_PTR(-EINVAL);
	if (WARN_ON(inode->i_size > PATH_MAX))
		return ERR_PTR(-EIO);

	// TODO: Do I need to grab some sort of lock here?
	if (inode->i_link)
		return inode->i_link;

	bv = hcasfs_inode_buffered_view(inode);
	if (IS_ERR(bv))
		return ERR_PTR(PTR_ERR(bv));

	link_data = kmalloc(inode->i_size + 1, GFP_KERNEL);

	pos = 0;
	read_data = buffered_view_read_full(bv, link_data, inode->i_size, &pos);
	if (IS_ERR(read_data)) {
		kfree(link_data);
		return read_data;
	} else if (read_data != link_data) {
		memcpy(link_data, read_data, inode->i_size);
	}
	link_data[inode->i_size] = 0;

	inode->i_link = link_data;
	return link_data;
}

const struct inode_operations hcasfs_dir_inode_ops = {
	.lookup = hcasfs_lookup,
};

const struct inode_operations hcasfs_lnk_inode_ops = {
	.get_link = hcasfs_get_link,
};

const struct inode_operations hcasfs_none_inode_ops = {};

int hcasfs_inode_has_content(struct inode *inode)
{
	return S_ISREG(inode->i_mode) || S_ISDIR(inode->i_mode) ||
	       S_ISLNK(inode->i_mode);
}
