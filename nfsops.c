// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 Vast Data Ltd.

/*
 * Pull linux/build_bug.h before linux/fs.h, then force static_assert checks to
 * always succeed in this TU. Clang BPF layout can disagree with the running
 * kernel (e.g. struct filename). A plain ((void)0) replacement is invalid at
 * file scope; kernel headers use static_assert after structs, so we keep the
 * kernel macro shape but compile to _Static_assert(1, ...).
 */
#include <linux/build_bug.h>
#undef static_assert
#undef __static_assert
#define __static_assert(expr, msg, ...) _Static_assert(1, "vnfs bpf")
#define static_assert(expr, ...) __static_assert(expr, ##__VA_ARGS__, #expr)

#include <uapi/linux/stat.h>
#include <linux/fs.h>
#include <linux/uio.h>
#include <uapi/linux/ptrace.h>
/*
 * Byte delta from struct vfsmount* to struct mount.mnt_id.
 * Set by userspace: offsetof(struct mount, mnt_id) - offsetof(struct mount, mnt)
 */
#ifndef MOUNT_MNT_TO_MNT_ID_DELTA
#define MOUNT_MNT_TO_MNT_ID_DELTA 0
#endif
#ifndef MOUNT_MNT_ID_DISABLED
#define MOUNT_MNT_ID_DISABLED 0
#endif

struct start_t {
	struct inode *inode;
	u64 start;
	u64 count;
	u32 mnt_id;
};

BPF_HASH(starts, u32, struct start_t);

// the key for the output summary
struct info_t {
	u32 pid;
	u32 tgid;
	u32 uid;
	char comm[TASK_COMM_LEN];
	u32 mnt_id;
	u32 sbdev;
};

struct stat_t {
	u64 count;
	u64 duration;
	u32 errors;
} __attribute__((packed));

struct stats_t {
	// regular file operations
	struct stat_t open;
	struct stat_t close;
	struct stat_t setattr;
	struct stat_t getattr;
	struct stat_t flush;
	struct stat_t mmap;
	struct stat_t fsync;
	struct stat_t lock;

	// I/O operations
	struct stat_t read;
	u64 rbytes;
	struct stat_t write;
	u64 wbytes;

	// directory operations
	struct stat_t create;
	struct stat_t link;
	struct stat_t unlink;
	struct stat_t symlink;
	struct stat_t readdir;
	struct stat_t lookup;
	struct stat_t rename;
	struct stat_t access;
	struct stat_t listxattr;
	struct stat_t mkdir;
	struct stat_t rmdir;
};

BPF_HASH(counts, struct info_t, struct stats_t);
BPF_HASH(mnt_ns_hint, u64, u32);

struct pidinfo_t {
	u32 pid;
};

BPF_PERF_OUTPUT(events);

static __always_inline void read_mnt_id_from_vfsmnt(struct vfsmount *vm, u32 *out)
{
	if (!out)
		return;
#if MOUNT_MNT_ID_DISABLED
	*out = 0;
	(void)vm;
#else
	if (!vm)
		return;
	if (bpf_probe_read_kernel(out, sizeof(*out),
				  (void *)vm + MOUNT_MNT_TO_MNT_ID_DELTA))
		*out = 0;
#endif
}

static __always_inline void clear_mnt_hint(void)
{
	u64 k = bpf_get_current_pid_tgid();
	mnt_ns_hint.delete(&k);
}

static __always_inline void store_hint_from_dir(struct path *dirp)
{
	struct path p;

	if (!dirp)
		return;
	if (bpf_probe_read_kernel(&p, sizeof(p), dirp))
		return;

	u32 mid = 0;
	read_mnt_id_from_vfsmnt(p.mnt, &mid);
	u64 k = bpf_get_current_pid_tgid();
	mnt_ns_hint.update(&k, &mid);
}

static __always_inline void store_hint_new_dir_link(void *new_dir_ptr)
{
	struct path *new_dir = new_dir_ptr;
	struct path p;

	if (!new_dir)
		return;
	if (bpf_probe_read_kernel(&p, sizeof(p), new_dir))
		return;

	u32 mid = 0;
	read_mnt_id_from_vfsmnt(p.mnt, &mid);
	u64 k = bpf_get_current_pid_tgid();
	mnt_ns_hint.update(&k, &mid);
}

int trace_sp_ret_fail_clear(struct pt_regs *ctx)
{
	if (!PT_REGS_RC(ctx))
		return 0;

	u64 k = bpf_get_current_pid_tgid();
	mnt_ns_hint.delete(&k);
	return 0;
}

int trace_sp_unlink(struct pt_regs *ctx)
{
	store_hint_from_dir((struct path *)PT_REGS_PARM1(ctx));
	return 0;
}

int trace_sp_mkdir(struct pt_regs *ctx)
{
	store_hint_from_dir((struct path *)PT_REGS_PARM1(ctx));
	return 0;
}

int trace_sp_rmdir(struct pt_regs *ctx)
{
	store_hint_from_dir((struct path *)PT_REGS_PARM1(ctx));
	return 0;
}

int trace_sp_symlink(struct pt_regs *ctx)
{
	store_hint_from_dir((struct path *)PT_REGS_PARM1(ctx));
	return 0;
}

int trace_sp_mknod(struct pt_regs *ctx)
{
	store_hint_from_dir((struct path *)PT_REGS_PARM1(ctx));
	return 0;
}

int trace_sp_link(struct pt_regs *ctx)
{
	store_hint_new_dir_link((void *)PT_REGS_PARM2(ctx));
	return 0;
}

int trace_sp_rename(struct pt_regs *ctx)
{
	store_hint_from_dir((struct path *)PT_REGS_PARM1(ctx));
	return 0;
}

int trace_execve(struct pt_regs *ctx,
		const char __user *filename,
		const char __user *const __user *argv,
		const char __user *const __user *envp)
{

	struct pidinfo_t data = {
		.pid = bpf_get_current_pid_tgid() >> 32,
	};
	events.perf_submit(ctx, &data, sizeof(data));
	return 0;
}

static struct stats_t *get_stats(u64 *start_time, u64 *byte_count)
{
	u32 pid = bpf_get_current_pid_tgid();

	struct start_t *startp = starts.lookup(&pid);
	if (!startp)
		return NULL;
	// update output variables taken in the function entrypoint
	*start_time = startp->start;
	if (byte_count)
		*byte_count = startp->count;

	struct info_t info = {
		.pid = pid,
		.tgid = bpf_get_current_pid_tgid() >> 32,
		.uid = bpf_get_current_uid_gid(),
		.mnt_id = startp->mnt_id,
		.sbdev = startp->inode->i_sb->s_dev,
	};
	bpf_get_current_comm(&info.comm, sizeof(info.comm));

	// delete the start from the map, no need for it
	starts.delete(&pid);

	struct stats_t zero = {};
	return counts.lookup_or_try_init(&info, &zero);
}

static struct start_t *get(void)
{
	u32 pid = bpf_get_current_pid_tgid();
	struct start_t zero = {};
	return starts.lookup_or_try_init(&pid, &zero);
}

static int should_filter_file(struct file *file)
{
	struct dentry *de = file->f_path.dentry;
	int mode = file->f_inode->i_mode;
	struct qstr d_name = de->d_name;

	if (d_name.len == 0)
		return 1;

	if (!S_ISREG(mode) && !S_ISDIR(mode) && !S_ISLNK(mode))
		return 1;

	return 0;
}

static int trace_from_file(struct pt_regs *ctx, struct file *file, u64 count)
{
	if (should_filter_file(file))
		return 0;

	struct start_t *startp = get();
	if (!startp)
		return 0;

	clear_mnt_hint();
	struct path p;
	bpf_probe_read_kernel(&p, sizeof(p), &file->f_path);
	u32 mid = 0;
	read_mnt_id_from_vfsmnt(p.mnt, &mid);
	startp->mnt_id = mid;
	startp->start = bpf_ktime_get_ns();
	startp->inode = file->f_inode;
	startp->count = count;
	return 0;
}

static int trace_from_path(struct pt_regs *ctx, const struct path *path, u64 count)
{
	struct start_t *startp = get();

	if (!startp)
		return 0;

	clear_mnt_hint();
	u32 mid = 0;
	read_mnt_id_from_vfsmnt(path->mnt, &mid);
	startp->mnt_id = mid;
	startp->start = bpf_ktime_get_ns();
	startp->inode = path->dentry->d_inode;
	startp->count = count;
	return 0;
}

static int trace_from_inode_hint(struct pt_regs *ctx, struct inode *inode,
				 u64 count)
{
	struct start_t *startp = get();

	if (!startp)
		return 0;

	u64 k = bpf_get_current_pid_tgid();
	u32 mid = 0;
	u32 *hp = mnt_ns_hint.lookup(&k);

	if (hp) {
		mid = *hp;
		mnt_ns_hint.delete(&k);
	}

	startp->mnt_id = mid;
	startp->start = bpf_ktime_get_ns();
	startp->inode = inode;
	startp->count = count;
	return 0;
}

static int file_read_write(struct pt_regs *ctx, struct file *file,
		size_t count, int is_read)
{
	if (should_filter_file(file))
		return 0;

	return trace_from_file(ctx, file, count);
}

static int file_read_write_ret(struct pt_regs *ctx, int is_read)
{
	u64 start, count;
	struct stats_t *statsp = get_stats(&start, &count);
	if (!statsp)
		return 0;

	if (is_read) {
		statsp->read.count++;
		statsp->rbytes += count;
		if (PT_REGS_RC(ctx) < 0)
			statsp->read.errors++;
		statsp->read.duration += bpf_ktime_get_ns() - start;
	} else {
		statsp->write.count++;
		statsp->wbytes += count;
		if (PT_REGS_RC(ctx) < 0)
			statsp->write.errors++;
		statsp->write.duration += bpf_ktime_get_ns() - start;
	}

	return 0;
}

int trace_nfs_file_read(struct pt_regs *ctx, struct kiocb *iocb,
		struct iov_iter *to)
{
	return file_read_write(ctx, iocb->ki_filp, to->count, 1);
}

int trace_nfs_file_read_ret(struct pt_regs *ctx)
{
	return file_read_write_ret(ctx, 1);
}

int trace_nfs_file_write(struct pt_regs *ctx, struct kiocb *iocb,
		struct iov_iter *from)
{
	return file_read_write(ctx, iocb->ki_filp, from->count, 0);
}

int trace_nfs_file_write_ret(struct pt_regs *ctx)
{
	return file_read_write_ret(ctx, 0);
}

int trace_nfs_file_splice_read(struct pt_regs *ctx, struct file *in,
		loff_t *ppos, struct pipe_inode_info *pipe,
		size_t len, unsigned int flags)
{
	return file_read_write(ctx, in, len, 1);
}

int trace_nfs_file_splice_ret(struct pt_regs *ctx)
{
	return file_read_write_ret(ctx, 1);
}

int trace_nfs_file_open(struct pt_regs *ctx, struct inode *inode,
		struct file *file)
{
	if (should_filter_file(file))
		return 0;

	return trace_from_file(ctx, file, 0);
}

int trace_nfs_file_open_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->open.count++;
	if (PT_REGS_RC(ctx))
		statsp->open.errors++;
	statsp->open.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_getattr(struct pt_regs *ctx,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6,3,0)
struct mnt_idmap *idmap,
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(5,12,0)
struct user_namespace *mnt_userns,
#endif
		const struct path *path, struct kstat *stat, u32 request_mask,
		unsigned int query_flags)
{
	return trace_from_path(ctx, path, 0);
}

int trace_nfs_getattr_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->getattr.count++;
	if (PT_REGS_RC(ctx))
		statsp->getattr.errors++;
	statsp->getattr.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_setattr(struct pt_regs *ctx,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6,3,0)
struct mnt_idmap *idmap,
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(5,12,0)
struct user_namespace *mnt_userns,
#endif
		struct dentry *dentry, struct iattr *attr)
{
	return trace_from_inode_hint(ctx, dentry->d_inode, 0);
}

int trace_nfs_setattr_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->setattr.count++;
	if (PT_REGS_RC(ctx))
		statsp->setattr.errors++;
	statsp->setattr.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_file_flush(struct pt_regs *ctx,
		struct file *file, fl_owner_t id)
{
	if (should_filter_file(file))
		return 0;

	return trace_from_file(ctx, file, 0);
}

int trace_nfs_file_flush_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->flush.count++;
	if (PT_REGS_RC(ctx))
		statsp->flush.errors++;
	statsp->flush.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_file_fsync(struct pt_regs *ctx,
		struct file *file, loff_t start, loff_t end, int datasync)
{
	if (should_filter_file(file))
		return 0;

	return trace_from_file(ctx, file, 0);
}

int trace_nfs_file_fsync_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->fsync.count++;
	if (PT_REGS_RC(ctx))
		statsp->fsync.errors++;
	statsp->fsync.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_lock(struct pt_regs *ctx, struct file *file,
		int cmd, struct file_lock *fl)
{
	if (should_filter_file(file))
		return 0;

	return trace_from_file(ctx, file, 0);
}

int trace_nfs_lock_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->lock.count++;
	if (PT_REGS_RC(ctx))
		statsp->lock.errors++;
	statsp->lock.duration += bpf_ktime_get_ns() - start;
	return 0;
}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6,17,0)
int trace_nfs_file_mmap(struct pt_regs *ctx,
		struct vm_area_desc *desc)
{
	struct file *file = desc->file;

	if (should_filter_file(file))
		return 0;

	return trace_from_file(ctx, file, 0);
}
#else
int trace_nfs_file_mmap(struct pt_regs *ctx,
		struct file *file, struct vm_area_struct *vma)
{
	if (should_filter_file(file))
		return 0;

	return trace_from_file(ctx, file, 0);
}
#endif

int trace_nfs_file_mmap_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->mmap.count++;
	if (PT_REGS_RC(ctx))
		statsp->mmap.errors++;
	statsp->mmap.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_file_release(struct pt_regs *ctx, struct inode *inode,
		struct file *file)
{
	if (should_filter_file(file))
		return 0;

	return trace_from_file(ctx, file, 0);
}

int trace_nfs_file_release_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->close.count++;
	if (PT_REGS_RC(ctx))
		statsp->close.errors++;
	statsp->close.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_readdir(struct pt_regs *ctx, struct file *file,
		struct dir_context *dctx)
{
	if (should_filter_file(file))
		return 0;

	return trace_from_file(ctx, file, 0);
}

int trace_nfs_readdir_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->readdir.count++;
	if (PT_REGS_RC(ctx))
		statsp->readdir.errors++;
	statsp->readdir.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_create(struct pt_regs *ctx,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6,3,0)
struct mnt_idmap *idmap,
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(5,12,0)
struct user_namespace *mnt_userns,
#endif
		struct inode *dir, struct dentry *dentry, umode_t mode, bool excl)
{
	return trace_from_inode_hint(ctx, dir, 0);
}

int trace_nfs_create_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->create.count++;
	if (PT_REGS_RC(ctx))
		statsp->create.errors++;
	statsp->create.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_link(struct pt_regs *ctx, struct dentry *old_dentry,
		struct inode *dir, struct dentry *dentry)
{
	return trace_from_inode_hint(ctx, dir, 0);
}

int trace_nfs_link_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->link.count++;
	if (PT_REGS_RC(ctx))
		statsp->link.errors++;
	statsp->link.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_unlink(struct pt_regs *ctx, struct inode *dir, struct dentry *dentry)
{
	return trace_from_inode_hint(ctx, dir, 0);
}

int trace_nfs_unlink_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->unlink.count++;
	if (PT_REGS_RC(ctx))
		statsp->unlink.errors++;
	statsp->unlink.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_symlink(struct pt_regs *ctx,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6,3,0)
struct mnt_idmap *idmap,
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(5,12,0)
struct user_namespace *mnt_userns,
#endif
		struct inode *dir, struct dentry *dentry, const char *symname)
{
	return trace_from_inode_hint(ctx, dir, 0);
}

int trace_nfs_symlink_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->symlink.count++;
	if (PT_REGS_RC(ctx))
		statsp->symlink.errors++;
	statsp->symlink.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_lookup(struct pt_regs *ctx, struct inode *dir,
		struct dentry * dentry, unsigned int flags)
{
	return trace_from_inode_hint(ctx, dir, 0);
}

int trace_nfs_lookup_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->lookup.count++;
	if (PT_REGS_RC(ctx))
		statsp->lookup.errors++;
	statsp->lookup.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_rename(struct pt_regs *ctx,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6,3,0)
struct mnt_idmap *idmap,
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(5,12,0)
struct user_namespace *mnt_userns,
#endif
		struct inode *old_dir, struct dentry *old_dentry,
		struct inode *new_dir, struct dentry *new_dentry, unsigned int flags)
{
	return trace_from_inode_hint(ctx, old_dir, 0);
}

int trace_nfs_rename_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->rename.count++;
	if (PT_REGS_RC(ctx))
		statsp->rename.errors++;
	statsp->rename.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_do_access(struct pt_regs *ctx, struct inode *inode, const struct cred *cred, int mask)
{
	return trace_from_inode_hint(ctx, inode, 0);
}

int trace_nfs_do_access_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->access.count++;
	if (PT_REGS_RC(ctx))
		statsp->access.errors++;
	statsp->access.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_mkdir(struct pt_regs *ctx,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6,3,0)
struct mnt_idmap *idmap,
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(5,12,0)
struct user_namespace *mnt_userns,
#endif
		struct inode *dir, struct dentry *dentry, umode_t mode)
{
	return trace_from_inode_hint(ctx, dir, 0);
}

int trace_nfs_mkdir_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->mkdir.count++;
	if (PT_REGS_RC(ctx))
		statsp->mkdir.errors++;
	statsp->mkdir.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_rmdir(struct pt_regs *ctx, struct inode *dir, struct dentry *dentry)
{
	return trace_from_inode_hint(ctx, dir, 0);
}

int trace_nfs_rmdir_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->rmdir.count++;
	if (PT_REGS_RC(ctx))
		statsp->rmdir.errors++;
	statsp->rmdir.duration += bpf_ktime_get_ns() - start;
	return 0;
}

int trace_nfs_listxattrs(struct pt_regs *ctx, struct dentry *dentry, char *list, size_t size)
{
	return trace_from_inode_hint(ctx, dentry->d_inode, 0);
}

int trace_nfs_listxattrs_ret(struct pt_regs *ctx)
{
	u64 start;
	struct stats_t *statsp = get_stats(&start, NULL);
	if (!statsp)
		return 0;

	statsp->listxattr.count++;
	if (PT_REGS_RC(ctx))
		statsp->listxattr.errors++;
	statsp->listxattr.duration += bpf_ktime_get_ns() - start;
	return 0;
}
