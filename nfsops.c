#include <uapi/linux/stat.h>
#include <linux/fs.h>
#include <linux/uio.h>
#include <uapi/linux/ptrace.h>

// the key for the output summary
struct info_t {
    u32 pid;
    u32 uid;
    char comm[TASK_COMM_LEN];
};

// the value of the output summary
struct val_t {
    // regular file operations
    u64 opens;
    u64 closes;
    u64 setattrs;
    u64 getattrs;
    u64 flushes;
    u64 mmaps;
    u64 fsyncs;
    u64 locks;

    // I/O operations
    u64 reads;
    u64 writes;
    u64 rbytes;
    u64 wbytes;

    // directory operations
    u64 creates;
    u64 links;
    u64 unlinks;
    u64 symlinks;
    u64 readdirs;
    u64 lookups;
    u64 renames;
    u64 accesses;
    u64 listxattrs;
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
        .pid = bpf_get_current_pid_tgid(),
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
    if (valp) {
        if (is_read) {
            valp->reads++;
            valp->rbytes += count;
        } else {
            valp->writes++;
            valp->wbytes += count;
        }
    }

    return 0;
}

int trace_nfs_file_read(struct pt_regs *ctx, struct kiocb *iocb,
        struct iov_iter *to)
{
    return file_read_write(ctx, iocb->ki_filp, to->count, 1);
}

int trace_nfs_file_write(struct pt_regs *ctx, struct kiocb *iocb,
        struct iov_iter *from)
{
    return file_read_write(ctx, iocb->ki_filp, from->count, 0);
}

int trace_nfs_file_splice_read(struct pt_regs *ctx, struct file *in,
        loff_t *ppos, struct pipe_inode_info *pipe,
        size_t len, unsigned int flags)
{
    return file_read_write(ctx, in, len, 1);
}

int trace_nfs_file_open(struct pt_regs *ctx, struct inode *inode,
        struct file *file)
{
    if (should_filter_file(file))
        return 0;

    struct val_t *valp = get();
    if (valp)
        valp->opens++;
    return 0;
}

int trace_nfs_getattr(struct pt_regs *ctx, struct mnt_idmap *idmap,
        const struct path *path, struct kstat *stat, u32 request_mask,
        unsigned int query_flags)
{
    struct val_t *valp = get();
    if (valp)
        valp->getattrs++;
    return 0;
}

int trace_nfs_setattr(struct pt_regs *ctx, struct mnt_idmap *idmap,
        struct dentry *dentry, struct iattr *attr)
{
    struct val_t *valp = get();
    if (valp)
        valp->setattrs++;
    return 0;
}

int trace_nfs_file_flush(struct pt_regs *ctx,
        struct file *file, fl_owner_t id)
{
    if (should_filter_file(file))
        return 0;

    struct val_t *valp = get();
    if (valp)
        valp->flushes++;
    return 0;
}

int trace_nfs_file_fsync(struct pt_regs *ctx,
        struct file *file, loff_t start, loff_t end, int datasync)
{
    if (should_filter_file(file))
        return 0;

    struct val_t *valp = get();
    if (valp)
        valp->fsyncs++;
    return 0;
}

int trace_nfs_lock(struct pt_regs *ctx, struct file *file,
        int cmd, struct file_lock *fl)
{
    if (should_filter_file(file))
        return 0;

    struct val_t *valp = get();
    if (valp)
        valp->locks++;
    return 0;
}

int trace_nfs_file_mmap(struct pt_regs *ctx, struct file *file,
        struct vm_area_struct *vma)
{
    if (should_filter_file(file))
        return 0;

    struct val_t *valp = get();
    if (valp)
        valp->mmaps++;
    return 0;
}

int trace_nfs_file_release(struct pt_regs *ctx, struct inode *inode,
        struct file *file)
{
    if (should_filter_file(file))
        return 0;

    struct val_t *valp = get();
    if (valp)
        valp->closes++;
    return 0;
}

int trace_nfs_readdir(struct pt_regs *ctx, struct file *file,
        struct dir_context *dctx)
{
    if (should_filter_file(file))
        return 0;

    struct val_t *valp = get();
    if (valp)
        valp->readdirs++;
    return 0;
}

int trace_nfs_create(struct pt_regs *ctx, struct mnt_idmap *idmap,
        struct inode *dir, struct dentry *dentry, umode_t mode, bool excl)
{
    struct val_t *valp = get();
    if (valp)
        valp->creates++;
    return 0;
}

int trace_nfs_link(struct pt_regs *ctx, struct dentry *old_dentry,
        struct inode *dir, struct dentry *dentry)
{
    struct val_t *valp = get();
    if (valp)
        valp->links++;
    return 0;
}

int trace_nfs_unlink(struct pt_regs *ctx, struct inode *dir, struct dentry *dentry)
{
    struct val_t *valp = get();
    if (valp)
        valp->unlinks++;
    return 0;
}

int trace_nfs_symlink(struct pt_regs *ctx, struct mnt_idmap *idmap,
        struct inode *dir, struct dentry *dentry, const char *symname)
{
    struct val_t *valp = get();
    if (valp)
        valp->symlinks++;
    return 0;
}

int trace_nfs_lookup(struct pt_regs *ctx, struct inode *dir,
        struct dentry * dentry, unsigned int flags)
{
    struct val_t *valp = get();
    if (valp)
        valp->lookups++;
    return 0;
}

int trace_nfs_rename(struct pt_regs *ctx,  struct mnt_idmap *idmap, struct inode *old_dir,
               struct dentry *old_dentry, struct inode *new_dir,
               struct dentry *new_dentry, unsigned int flags)
{
    struct val_t *valp = get();
    if (valp)
        valp->renames++;
    return 0;
}

int trace_nfs_do_access(struct pt_regs *ctx, struct inode *inode, const struct cred *cred, int mask)
{
    struct val_t *valp = get();
    if (valp)
        valp->accesses++;
    return 0;
}

int trace_nfs_listxattrs(struct pt_regs *ctx, struct dentry *dentry, char *list, size_t size)
{
    struct val_t *valp = get();
    if (valp)
        valp->listxattrs++;
    return 0;
}
