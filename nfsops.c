#include <uapi/linux/stat.h>
#include <linux/fs.h>
#include <linux/uio.h>
#include <uapi/linux/ptrace.h>

// the key for the output summary
struct info_t {
	u32 pid;
	u32 tgid;
	u32 uid;
	char comm[TASK_COMM_LEN];
};

struct stat_t {
	u32 count;
	u32 errors;
	u64 start;
	u64 duration;
};

// the value of the output summary
struct val_t {
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
};

BPF_HASH(counts, struct info_t, struct val_t);

struct pidinfo_t {
	u32 pid;
};

BPF_PERF_OUTPUT(events);

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

static int should_filter_file(struct file *file)
{
	struct dentry *de = file->f_path.dentry;
	int mode = file->f_inode->i_mode;
	struct qstr d_name = de->d_name;

	// skip I/O lacking a filename
	if (d_name.len == 0)
		return 1;

	if (!S_ISREG(mode) && !S_ISDIR(mode) && !S_ISLNK(mode))
		return 1;

	return 0;
}

static struct val_t *get()
{
	struct info_t info = {
		.pid = bpf_get_current_pid_tgid(),
		.tgid = bpf_get_current_pid_tgid() >> 32,
		.uid = bpf_get_current_uid_gid(),
	};
	bpf_get_current_comm(&info.comm, sizeof(info.comm));

	struct val_t zero = {};
	return counts.lookup_or_try_init(&info, &zero);
}

static int file_read_write(struct pt_regs *ctx, struct file *file,
		size_t count, int is_read)
{
	if (should_filter_file(file))
		return 0;

	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (is_read) {
		valp->read.count++;
		valp->rbytes += count;
		valp->read.start = bpf_ktime_get_ns();
	} else {
		valp->write.count++;
		valp->wbytes += count;
		valp->write.start = bpf_ktime_get_ns();
	}

	return 0;
}

static int file_read_write_ret(struct pt_regs *ctx, int is_read)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (is_read) {
		if (PT_REGS_RC(ctx) < 0)
			valp->read.errors++;
		valp->read.duration += bpf_ktime_get_ns() - valp->read.start;
	} else {
		if (PT_REGS_RC(ctx) < 0)
			valp->write.errors++;
		valp->write.duration += bpf_ktime_get_ns() - valp->write.start;
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

	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->open.count++;
	valp->open.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_file_open_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->open.errors++;
	valp->open.duration += bpf_ktime_get_ns() - valp->open.start;
	return 0;
}

int trace_nfs_getattr(struct pt_regs *ctx, struct mnt_idmap *idmap,
		const struct path *path, struct kstat *stat, u32 request_mask,
		unsigned int query_flags)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->getattr.count++;
	valp->getattr.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_getattr_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->getattr.errors++;
	valp->getattr.duration += bpf_ktime_get_ns() - valp->getattr.start;
	return 0;
}

int trace_nfs_setattr(struct pt_regs *ctx, struct mnt_idmap *idmap,
		struct dentry *dentry, struct iattr *attr)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->setattr.count++;
	valp->setattr.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_setattr_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->setattr.errors++;
	valp->setattr.duration += bpf_ktime_get_ns() - valp->setattr.start;
	return 0;
}

int trace_nfs_file_flush(struct pt_regs *ctx,
		struct file *file, fl_owner_t id)
{
	if (should_filter_file(file))
		return 0;

	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->flush.count++;
	valp->flush.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_file_flush_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->flush.errors++;
	valp->flush.duration += bpf_ktime_get_ns() - valp->flush.start;
	return 0;
}

int trace_nfs_file_fsync(struct pt_regs *ctx,
		struct file *file, loff_t start, loff_t end, int datasync)
{
	if (should_filter_file(file))
		return 0;

	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->fsync.count++;
	valp->fsync.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_file_fsync_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->fsync.errors++;
	valp->fsync.duration += bpf_ktime_get_ns() - valp->fsync.start;
	return 0;
}

int trace_nfs_lock(struct pt_regs *ctx, struct file *file,
		int cmd, struct file_lock *fl)
{
	if (should_filter_file(file))
		return 0;

	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->lock.count++;
	valp->lock.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_lock_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->lock.errors++;
	valp->lock.duration += bpf_ktime_get_ns() - valp->lock.start;
	return 0;
}

int trace_nfs_file_mmap(struct pt_regs *ctx, struct file *file,
		struct vm_area_struct *vma)
{
	if (should_filter_file(file))
		return 0;

	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->mmap.count++;
	valp->mmap.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_file_mmap_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->mmap.errors++;
	valp->mmap.duration += bpf_ktime_get_ns() - valp->mmap.start;
	return 0;
}

int trace_nfs_file_release(struct pt_regs *ctx, struct inode *inode,
		struct file *file)
{
	if (should_filter_file(file))
		return 0;

	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->close.count++;
	valp->close.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_file_release_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->close.errors++;
	valp->close.duration += bpf_ktime_get_ns() - valp->close.start;
	return 0;
}

int trace_nfs_readdir(struct pt_regs *ctx, struct file *file,
		struct dir_context *dctx)
{
	if (should_filter_file(file))
		return 0;

	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->readdir.count++;
	valp->readdir.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_readdir_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->readdir.errors++;
	valp->readdir.duration += bpf_ktime_get_ns() - valp->readdir.start;
	return 0;
}

int trace_nfs_create(struct pt_regs *ctx, struct mnt_idmap *idmap,
		struct inode *dir, struct dentry *dentry, umode_t mode, bool excl)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->create.count++;
	valp->create.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_create_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->create.errors++;
	valp->create.duration += bpf_ktime_get_ns() - valp->create.start;
	return 0;
}

int trace_nfs_link(struct pt_regs *ctx, struct dentry *old_dentry,
		struct inode *dir, struct dentry *dentry)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->link.count++;
	valp->link.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_link_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->link.errors++;
	valp->link.duration += bpf_ktime_get_ns() - valp->link.start;
	return 0;
}

int trace_nfs_unlink(struct pt_regs *ctx, struct inode *dir, struct dentry *dentry)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->unlink.count++;
	valp->unlink.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_unlink_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->unlink.errors++;
	valp->unlink.duration += bpf_ktime_get_ns() - valp->unlink.start;
	return 0;
}

int trace_nfs_symlink(struct pt_regs *ctx, struct mnt_idmap *idmap,
		struct inode *dir, struct dentry *dentry, const char *symname)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->symlink.count++;
	valp->symlink.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_symlink_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->symlink.errors++;
	valp->symlink.duration += bpf_ktime_get_ns() - valp->symlink.start;
	return 0;
}

int trace_nfs_lookup(struct pt_regs *ctx, struct inode *dir,
		struct dentry * dentry, unsigned int flags)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->lookup.count++;
	valp->lookup.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_lookup_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->lookup.errors++;
	valp->lookup.duration += bpf_ktime_get_ns() - valp->lookup.start;
	return 0;
}

int trace_nfs_rename(struct pt_regs *ctx,  struct mnt_idmap *idmap, struct inode *old_dir,
			   struct dentry *old_dentry, struct inode *new_dir,
			   struct dentry *new_dentry, unsigned int flags)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;
	valp->rename.count++;
	valp->rename.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_rename_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->rename.errors++;
	valp->rename.duration += bpf_ktime_get_ns() - valp->rename.start;
	return 0;
}

int trace_nfs_do_access(struct pt_regs *ctx, struct inode *inode, const struct cred *cred, int mask)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;
	valp->access.count++;
	valp->access.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_do_access_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->access.errors++;
	valp->access.duration += bpf_ktime_get_ns() - valp->access.start;
	return 0;
}


int trace_nfs_listxattrs(struct pt_regs *ctx, struct dentry *dentry, char *list, size_t size)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	valp->listxattr.count++;
	valp->listxattr.start = bpf_ktime_get_ns();
	return 0;
}

int trace_nfs_listxattrs_ret(struct pt_regs *ctx)
{
	struct val_t *valp = get();
	if (!valp)
		return 0;

	if (PT_REGS_RC(ctx))
		valp->listxattr.errors++;
	valp->listxattr.duration += bpf_ktime_get_ns() - valp->listxattr.start;
	return 0;
}
