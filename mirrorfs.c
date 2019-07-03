#define FUSE_USE_VERSION 31

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <assert.h>
#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>

#define ABORT_IF_NOT_EQUAL(x, y) \
    do { \
        long _x = (x); \
        long _y = (y); \
        if (_x != _y) { \
            fprintf(stderr, "%s: %ld != %ld\n", __func__, _x, _y); \
            if (abort_on_difference) { \
                abort(); \
            } \
        } \
    } while (0)

#define ABORT_IF_INCONSISTENT_FD(fd1, fd2) \
    do { \
        if (((fd1) == -1) ^ ((fd2) == -1)) { \
            fprintf(stderr, "%s: %d != %d\n", __func__, (fd1), (fd2)); \
            abort(); \
        } \
    } while (0)

#define LOG_FUSE_OPERATION(fmt, ...) \
    do { \
        if (log_operations) { \
            fprintf(stderr, "%s: " fmt "\n", __func__, __VA_ARGS__); \
        } \
    } while (0)

// TODO: add flags to configure these
static int abort_on_difference = 1;
static int log_operations = 1;

static const char *mntpath1 = NULL;
static const char *mntpath2 = NULL;
static int mntfd1 = -1;
static int mntfd2 = -1;
static int mirror_fds[1024];  // TODO: hardcoded limit

// FUSE delivers paths with a leading slash.  Remove them when possible and
// return dot otherwise.
static const char *safe_path(const char *path)
{
    if (strcmp(path, "/") == 0) {
        return ".";
    }
    return path + 1;
}

static void *mirrorfs_init(struct fuse_conn_info *conn,
                           struct fuse_config *cfg)
{
    cfg->use_ino = 1;

    /* Pick up changes from lower filesystem right away. This is
       also necessary for better hardlink support. When the kernel
       calls the unlink() handler, it does not know the inode of
       the to-be-removed entry and can therefore not invalidate
       the cache of the associated inode - resulting in an
       incorrect st_nlink value being reported for any remaining
       hardlinks to this inode. */
    cfg->entry_timeout = 0;
    cfg->attr_timeout = 0;
    cfg->negative_timeout = 0;

    return NULL;
}

static int mirrorfs_getattr(const char *path, struct stat *stbuf,
                            struct fuse_file_info *fi)
{
    LOG_FUSE_OPERATION("%s", path);

    struct stat stbuf2;
    memset(&stbuf2, 0, sizeof(stbuf2));

    errno = 0;
    int res1 = fstatat(mntfd1, safe_path(path), stbuf, AT_SYMLINK_NOFOLLOW);
    int errno1 = errno;

    errno = 0;
    int res2 = fstatat(mntfd2, safe_path(path), &stbuf2, AT_SYMLINK_NOFOLLOW);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    if (memcmp(stbuf, &stbuf2, sizeof(*stbuf)) != 0) {
        // do not compare st_dev
        // do not compare st_ino
        ABORT_IF_NOT_EQUAL(stbuf->st_mode, stbuf2.st_mode);
        ABORT_IF_NOT_EQUAL(stbuf->st_nlink, stbuf2.st_nlink);
        ABORT_IF_NOT_EQUAL(stbuf->st_uid, stbuf2.st_uid);
        ABORT_IF_NOT_EQUAL(stbuf->st_gid, stbuf2.st_gid);
        // do not compare st_rdev
        if(!S_ISDIR(stbuf->st_mode)){
            ABORT_IF_NOT_EQUAL(stbuf->st_size, stbuf2.st_size);
        }
        // do not compare st_blksize
        // do not compare st_blocks
        // do not compare st_atim
        // do not compare st_mtim
        // do not compare st_ctim
    }

    return 0;
}

static int mirrorfs_access(const char *path, int mask)
{
    LOG_FUSE_OPERATION("%s 0x%x", path, mask);

    errno = 0;
    int res1 = faccessat(mntfd1, safe_path(path), mask, 0);
    int errno1 = errno;

    errno = 0;
    int res2 = faccessat(mntfd2, safe_path(path), mask, 0);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    return 0;
}

static int mirrorfs_readlink(const char *path, char *buf, size_t size)
{
    LOG_FUSE_OPERATION("%s %zu", path, size);

    errno = 0;
    int res1 = readlinkat(mntfd1, safe_path(path), buf, size - 1);
    int errno1 = errno;

    errno = 0;
    int res2 = readlinkat(mntfd2, safe_path(path), buf, size - 1);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    buf[res1] = '\0';
    return 0;
}

// TODO: incomplete; compare against dir2fd.  how to handle different directory
// orders?
static int mirrorfs_readdir(const char *path, void *buf,
                            fuse_fill_dir_t filler, off_t offset,
                            struct fuse_file_info *fi,
                            enum fuse_readdir_flags flags)
{
    LOG_FUSE_OPERATION("%s %ld 0x%x", path, offset, flags);

    struct dirent *de;

    int dir1fd = openat(mntfd1, safe_path(path), O_DIRECTORY);
    int dir2fd = openat(mntfd2, safe_path(path), O_DIRECTORY);
    ABORT_IF_INCONSISTENT_FD(dir1fd, dir2fd);

    DIR *dp1 = fdopendir(dir1fd);
    DIR *dp2 = fdopendir(dir2fd);
    if (dp1 == NULL || dp2 == NULL) {
        return -errno;
    }

    while ((de = readdir(dp1)) != NULL) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = de->d_ino;
        st.st_mode = de->d_type << 12;
        if (filler(buf, de->d_name, &st, 0, 0)) {
            break;
        }
    }

    closedir(dp1);
    closedir(dp2);
    close(dir1fd);
    close(dir2fd);
    return 0;
}

static int mirrorfs_mkdir(const char *path, mode_t mode)
{
    LOG_FUSE_OPERATION("%s 0x%x", path, mode);

    errno = 0;
    int res1 = mkdirat(mntfd1, safe_path(path), mode);
    int errno1 = errno;

    errno = 0;
    int res2 = mkdirat(mntfd2, safe_path(path), mode);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    return 0;
}

static int mirrorfs_unlink(const char *path)
{
    LOG_FUSE_OPERATION("%s", path);

    errno = 0;
    int res1 = unlinkat(mntfd1, safe_path(path), 0);
    int errno1 = errno;

    errno = 0;
    int res2 = unlinkat(mntfd2, safe_path(path), 0);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    return 0;
}

static int mirrorfs_rmdir(const char *path)
{
    LOG_FUSE_OPERATION("%s", path);

    errno = 0;
    int res1 = unlinkat(mntfd1, path, AT_REMOVEDIR);
    int errno1 = errno;

    errno = 0;
    int res2 = unlinkat(mntfd2, path, AT_REMOVEDIR);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    return 0;
}

static int mirrorfs_symlink(const char *from, const char *to)
{
    LOG_FUSE_OPERATION("%s %s", from, to);

    errno = 0;
    int res1 = symlinkat(from, mntfd1, safe_path(to));
    int errno1 = errno;

    errno = 0;
    int res2 = symlinkat(from, mntfd2, safe_path(to));
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    return 0;
}

static int mirrorfs_rename(const char *from, const char *to,
                           unsigned int flags)
{
    LOG_FUSE_OPERATION("%s %s 0x%x", from, to, flags);

    if (flags) {
        return -EINVAL;
    }

    errno = 0;
    int res1 = renameat(mntfd1, safe_path(from), mntfd1, safe_path(to));
    int errno1 = errno;

    errno = 0;
    int res2 = renameat(mntfd2, safe_path(from), mntfd2, safe_path(to));
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    return 0;
}

static int mirrorfs_link(const char *from, const char *to)
{
    LOG_FUSE_OPERATION("%s %s", from, to);

    errno = 0;
    int res1 = linkat(mntfd1, from, mntfd1, to, 0);
    int errno1 = errno;

    errno = 0;
    int res2 = linkat(mntfd1, from, mntfd1, to, 0);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    return 0;
}

static int mirrorfs_chmod(const char *path, mode_t mode,
                          struct fuse_file_info *fi)
{
    LOG_FUSE_OPERATION("%s 0x%x", path, mode);

    errno = 0;
    int res1 = fchmodat(mntfd1, safe_path(path), mode, 0);
    int errno1 = errno;

    errno = 0;
    int res2 = fchmodat(mntfd2, safe_path(path), mode, 0);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    return 0;
}

static int mirrorfs_chown(const char *path, uid_t uid, gid_t gid,
                          struct fuse_file_info *fi)
{
    LOG_FUSE_OPERATION("%s %d %d", path, uid, gid);

    errno = 0;
    int res1 = fchownat(mntfd1, safe_path(path), uid, gid, 0);
    int errno1 = errno;

    errno = 0;
    int res2 = fchownat(mntfd2, safe_path(path), uid, gid, 0);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    return 0;
}

// TODO: not implemented: call open and ftruncate whe fi is NULL?
static int mirrorfs_truncate(const char *path, off_t size,
                             struct fuse_file_info *fi)
{
    int res;

    if (fi != NULL) {
        res = ftruncate(fi->fh, size);
    } else {
        res = truncate(path, size);
    }
    if (res == -1) {
        return -errno;
    }

    return 0;
}

static int mirrorfs_utimens(const char *path, const struct timespec ts[2],
                            struct fuse_file_info *fi)
{
    LOG_FUSE_OPERATION("%s", path);

    errno = 0;
    int res1 = utimensat(mntfd1, safe_path(path), ts, AT_SYMLINK_NOFOLLOW);
    int errno1 = errno;

    errno = 0;
    int res2 = utimensat(mntfd2, safe_path(path), ts, AT_SYMLINK_NOFOLLOW);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        return -errno;
    }

    return 0;
}

static int mirrorfs_create(const char *path, mode_t mode,
                           struct fuse_file_info *fi)
{
    LOG_FUSE_OPERATION("%s %o 0x%x", path, mode, fi->flags);

    errno = 0;
    int fd1 = openat(mntfd1, safe_path(path), fi->flags, mode);
    int errno1 = errno;

    errno = 0;
    int fd2 = openat(mntfd2, safe_path(path), fi->flags, mode);
    int errno2 = errno;

    ABORT_IF_INCONSISTENT_FD(fd1, fd2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (fd1 == -1) {
        return -errno;
    }

    fi->fh = fd1;
    assert(mirror_fds[fi->fh] == -1);
    mirror_fds[fi->fh] = fd2;
    return 0;
}

static int mirrorfs_open(const char *path, struct fuse_file_info *fi)
{
    LOG_FUSE_OPERATION("%s", path);

    errno = 0;
    int fd1 = openat(mntfd1, safe_path(path), fi->flags);
    int errno1 = errno;

    errno = 0;
    int fd2 = openat(mntfd2, safe_path(path), fi->flags);
    int errno2 = errno;

    ABORT_IF_INCONSISTENT_FD(fd1, fd2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (fd1 == -1) {
        return -errno;
    }

    fi->fh = fd1;
    assert(mirror_fds[fi->fh] == -1);
    mirror_fds[fi->fh] = fd2;
    return 0;
}

static int mirrorfs_read(const char *path, char *buf, size_t size,
                         off_t offset, struct fuse_file_info *fi)
{
    LOG_FUSE_OPERATION("%s %zu %ld %p", path, size, offset, fi);

    int fd1;
    int fd2;

    if (fi == NULL) {
        fd1 = openat(mntfd1, safe_path(path), O_RDONLY);
        fd2 = openat(mntfd2, safe_path(path), O_RDONLY);
    } else {
        fd1 = fi->fh;
        fd2 = mirror_fds[fi->fh];
    }

    if (fd1 == -1 || fd2 == -1) {
        return -errno;
    }

    char *buf2 = malloc(size);

    errno = 0;
    int res1 = pread(fd1, buf, size, offset);
    int errno1 = errno;

    errno = 0;
    int res2 = pread(fd2, buf2, size, offset);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        res1 = -errno;
    } else if (memcmp(buf, buf2, res1) != 0) {
        abort();
    }

    free(buf2);

    if (fi == NULL) {
        close(fd1);
        close(fd2);
    }
    return res1;
}

static int mirrorfs_write(const char *path, const char *buf, size_t size,
                          off_t offset, struct fuse_file_info *fi)
{
    LOG_FUSE_OPERATION("%s %lu %ld", path, size, offset);

    int fd1;
    int fd2;

    if (fi == NULL) {
        fd1 = openat(mntfd1, safe_path(path), O_WRONLY);
        fd2 = openat(mntfd2, safe_path(path), O_WRONLY);
    } else {
        fd1 = fi->fh;
        fd2 = mirror_fds[fi->fh];
    }

    ABORT_IF_INCONSISTENT_FD(fd1, fd2);
    if (fd1 == -1) {
        return -errno;
    }

    errno = 0;
    int res1 = pwrite(fd1, buf, size, offset);
    int errno1 = errno;

    errno = 0;
    int res2 = pwrite(fd2, buf, size, offset);
    int errno2 = errno;

    ABORT_IF_NOT_EQUAL(res1, res2);
    ABORT_IF_NOT_EQUAL(errno1, errno2);
    if (res1 == -1) {
        res1 = -errno;
    }

    if (fi == NULL) {
        close(fd1);
        close(fd2);
    }
    return res1;
}

static int mirrorfs_release(const char *path, struct fuse_file_info *fi)
{
    LOG_FUSE_OPERATION("%s", path);

    close(mirror_fds[fi->fh]);
    mirror_fds[fi->fh] = -1;
    // Must close fd1 after fd2 since mirror_fds uses fd1 as a key.
    close(fi->fh);
    return 0;
}

static int mirrorfs_fsync(const char *path, int isdatasync,
             struct fuse_file_info *fi)
{
    LOG_FUSE_OPERATION("%s %d", path, isdatasync);

    return 0;
}

static const struct fuse_operations mirrorfs_oper = {
    .init = mirrorfs_init,
    .getattr = mirrorfs_getattr,
    .access = mirrorfs_access,
    .readlink = mirrorfs_readlink,
    .readdir = mirrorfs_readdir,
    .mkdir = mirrorfs_mkdir,
    .symlink = mirrorfs_symlink,
    .unlink = mirrorfs_unlink,
    .rmdir = mirrorfs_rmdir,
    .rename = mirrorfs_rename,
    .link = mirrorfs_link,
    .chmod = mirrorfs_chmod,
    .chown = mirrorfs_chown,
    //.truncate = mirrorfs_truncate,
    .utimens = mirrorfs_utimens,
    .open = mirrorfs_open,
    .create = mirrorfs_create,
    .read = mirrorfs_read,
    .write = mirrorfs_write,
    .release = mirrorfs_release,
    .fsync = mirrorfs_fsync,
};

static int mirrorfs_opt_proc(void *data, const char *arg, int key,
                             struct fuse_args *outargs)
{
    if (key == FUSE_OPT_KEY_NONOPT) {
        if (mntpath1 == NULL) {
            mntpath1 = arg;
            return 0;
        } else if (mntpath2 == NULL) {
            mntpath2 = arg;
            return 0;
        } else {
            return 1;
        }
    }
    return 1;
}

int main(int argc, char *argv[])
{
    memset(mirror_fds, -1, sizeof(mirror_fds));
    umask(0);
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    int res = fuse_opt_parse(&args, NULL, NULL, mirrorfs_opt_proc);
    if (res != 0 || mntpath1 == NULL || mntpath2 == NULL) {
        // TODO: hook into FUSE help mechanism?
        printf("Usage: mntpath1 mntpath2 mountpoint\n");
        return 1;
    }
    mntfd1 = open(mntpath1, O_DIRECTORY);
    if (mntfd1 == -1) {
        perror("could not open mntpath1");
        return 1;
    }
    mntfd2 = open(mntpath2, O_DIRECTORY);
    if (mntfd2 == -1) {
        perror("could not open mntpath2");
        return 1;
    }
    return fuse_main(args.argc, args.argv, &mirrorfs_oper, NULL);
}
