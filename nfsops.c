#include <uapi/linux/stat.h>
#include <linux/fs.h>
#include <linux/uio.h>
#include <uapi/linux/ptrace.h>

struct start_t {
	struct inode *inode;
	u64 start;
	u64 count;
};

BPF_HASH(starts, u32, struct start_t);

// the key for the output summary
struct info_t {
	u32 pid;
	u32 tgid;
	u32 uid;
	char comm[TASK_COMM_LEN];
	u32 sbdev;
};

struct stat_t {
	u64 count;
	u64 duration;
	u32 errors;
} __attribute__((packed));

// the value of the output summary
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
};

BPF_HASH(counts, struct info_t, struct stats_t);

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
	//u32 s_dev = startp->inode->i_sb->s_dev;
	// delete the start from the map, no need for it
	starts.delete(&pid);

	struct info_t info = {
		.pid = pid,
		.tgid = bpf_get_current_pid_tgid() >> 32,
		.uid = bpf_get_current_uid_gid(),
		.sbdev = startp->inode->i_sb->s_dev,
	};
	bpf_get_current_comm(&info.comm, sizeof(info.comm));

	struct stats_t zero = {};
	return counts.lookup_or_try_init(&info, &zero);
}

static struct start_t *get()
{
	u32 pid = bpf_get_current_pid_tgid();
	struct start_t zero = {};
	return starts.lookup_or_try_init(&pid, &zero);
}

static int trace_nfs_function_entry(struct pt_regs *ctx,
		struct inode *inode, u64 count)
{
	struct start_t *startp = get();
	if (!startp)
		return 0;

	startp->start = bpf_ktime_get_ns();
	startp->inode = inode;
	startp->count = count;
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

static int file_read_write(struct pt_regs *ctx, struct file *file,
		size_t count, int is_read)
{
	if (should_filter_file(file))
		return 0;

	return trace_nfs_function_entry(ctx, file->f_inode, count);
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

	return trace_nfs_function_entry(ctx, inode, 0);
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

int trace_nfs_getattr(struct pt_regs *ctx, struct mnt_idmap *idmap,
		const struct path *path, struct kstat *stat, u32 request_mask,
		unsigned int query_flags)
{
	return trace_nfs_function_entry(ctx, path->dentry->d_inode, 0);
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

int trace_nfs_setattr(struct pt_regs *ctx, struct mnt_idmap *idmap,
		struct dentry *dentry, struct iattr *attr)
{
	return trace_nfs_function_entry(ctx, dentry->d_inode, 0);
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

	return trace_nfs_function_entry(ctx, file->f_inode, 0);
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

	return trace_nfs_function_entry(ctx, file->f_inode, 0);
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

	return trace_nfs_function_entry(ctx, file->f_inode, 0);
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

int trace_nfs_file_mmap(struct pt_regs *ctx, struct file *file,
		struct vm_area_struct *vma)
{
	if (should_filter_file(file))
		return 0;

	return trace_nfs_function_entry(ctx, file->f_inode, 0);
}

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

	return trace_nfs_function_entry(ctx, file->f_inode, 0);
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

	return trace_nfs_function_entry(ctx, file->f_inode, 0);
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

int trace_nfs_create(struct pt_regs *ctx, struct mnt_idmap *idmap,
		struct inode *dir, struct dentry *dentry, umode_t mode, bool excl)
{
	return trace_nfs_function_entry(ctx, dentry->d_inode, 0);
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
	return trace_nfs_function_entry(ctx, dentry->d_inode, 0);
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
	return trace_nfs_function_entry(ctx, dentry->d_inode, 0);
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

int trace_nfs_symlink(struct pt_regs *ctx, struct mnt_idmap *idmap,
		struct inode *dir, struct dentry *dentry, const char *symname)
{
	return trace_nfs_function_entry(ctx, dentry->d_inode, 0);
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
	return trace_nfs_function_entry(ctx, dentry->d_inode, 0);
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

int trace_nfs_rename(struct pt_regs *ctx,  struct mnt_idmap *idmap, struct inode *old_dir,
			   struct dentry *old_dentry, struct inode *new_dir,
			   struct dentry *new_dentry, unsigned int flags)
{
	return trace_nfs_function_entry(ctx, new_dentry->d_inode, 0);
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
	return trace_nfs_function_entry(ctx, inode, 0);
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


int trace_nfs_listxattrs(struct pt_regs *ctx, struct dentry *dentry, char *list, size_t size)
{
	return trace_nfs_function_entry(ctx, dentry->d_inode, 0);
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
