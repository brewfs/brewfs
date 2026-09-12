#ifndef BREWFS_H
#define BREWFS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BREWFS_ABI_V1_MAJOR 1u
#define BREWFS_ABI_V1_MINOR 0u
#define BREWFS_ABI_V1_VERSION ((BREWFS_ABI_V1_MAJOR << 16) | BREWFS_ABI_V1_MINOR)

#define BREWFS_OPEN_READ      (1u << 0)
#define BREWFS_OPEN_WRITE     (1u << 1)
#define BREWFS_OPEN_CREATE    (1u << 2)
#define BREWFS_OPEN_TRUNCATE  (1u << 3)
#define BREWFS_OPEN_EXCLUSIVE (1u << 4)
#define BREWFS_OPEN_APPEND    (1u << 5)

typedef enum brewfs_status_t {
    BREWFS_OK = 0,
    BREWFS_END = 1,
    BREWFS_BUFFER_TOO_SMALL = 2,
    BREWFS_INVALID_ARGUMENT = -1,
    BREWFS_INVALID_HANDLE = -2,
    BREWFS_NOT_FOUND = -3,
    BREWFS_ALREADY_EXISTS = -4,
    BREWFS_NOT_A_DIRECTORY = -5,
    BREWFS_IS_A_DIRECTORY = -6,
    BREWFS_DIRECTORY_NOT_EMPTY = -7,
    BREWFS_PERMISSION_DENIED = -8,
    BREWFS_READ_ONLY = -9,
    BREWFS_NO_SPACE = -10,
    BREWFS_UNSUPPORTED = -11,
    BREWFS_STALE_HANDLE = -12,
    BREWFS_IO_ERROR = -13,
    BREWFS_INTERNAL_ERROR = -14,
    BREWFS_PANIC = -15
} brewfs_status_t;

typedef struct brewfs_client brewfs_client_t;
typedef struct brewfs_file brewfs_file_t;
typedef struct brewfs_dir brewfs_dir_t;

typedef struct brewfs_client_options_v1 {
    uint32_t struct_size;
    uint32_t flags;
    const uint8_t *data_dir;
    size_t data_dir_len;
    const uint8_t *metadata_url;
    size_t metadata_url_len;
    uint64_t chunk_size;
    uint32_t block_size;
    uint32_t uid;
    uint32_t gid;
    uint8_t enforce_permissions;
    uint8_t reserved[3];
} brewfs_client_options_v1;

typedef struct brewfs_open_options_v1 {
    uint32_t struct_size;
    uint32_t flags;
    uint32_t mode;
} brewfs_open_options_v1;

typedef struct brewfs_stat_v1 {
    uint32_t struct_size;
    uint32_t file_type;
    uint64_t inode;
    uint64_t size;
    uint64_t blocks;
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    uint32_t rdev;
    uint32_t nlink;
    int64_t atime_ns;
    int64_t mtime_ns;
    int64_t ctime_ns;
} brewfs_stat_v1;

typedef struct brewfs_statfs_v1 {
    uint32_t struct_size;
    uint64_t total_space;
    uint64_t available_space;
    uint64_t used_space;
    uint64_t total_inodes;
    uint64_t available_inodes;
    uint64_t used_inodes;
} brewfs_statfs_v1;

typedef struct brewfs_setattr_v1 {
    uint32_t struct_size;
    uint32_t valid_mask;
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    uint64_t size;
    int64_t atime_ns;
    int64_t mtime_ns;
} brewfs_setattr_v1;

uint32_t brewfs_v1_abi_version(void);
brewfs_status_t brewfs_v1_client_open(
    const brewfs_client_options_v1 *options, brewfs_client_t **out_client);
brewfs_status_t brewfs_v1_client_close(brewfs_client_t *client);

brewfs_status_t brewfs_v1_open(
    brewfs_client_t *client, const uint8_t *path, size_t path_len,
    const brewfs_open_options_v1 *options, brewfs_file_t **out_file);
brewfs_status_t brewfs_v1_file_close(brewfs_file_t *file);
brewfs_status_t brewfs_v1_read(
    brewfs_file_t *file, uint8_t *buffer, size_t capacity, size_t *out_read);
brewfs_status_t brewfs_v1_pread(
    brewfs_file_t *file, uint64_t offset, uint8_t *buffer,
    size_t capacity, size_t *out_read);
brewfs_status_t brewfs_v1_write(
    brewfs_file_t *file, const uint8_t *data, size_t length, size_t *out_written);
brewfs_status_t brewfs_v1_pwrite(
    brewfs_file_t *file, uint64_t offset, const uint8_t *data,
    size_t length, size_t *out_written);
brewfs_status_t brewfs_v1_seek(brewfs_file_t *file, uint64_t offset);
brewfs_status_t brewfs_v1_flush(brewfs_file_t *file);
brewfs_status_t brewfs_v1_fsync(brewfs_file_t *file, uint8_t data_only);

brewfs_status_t brewfs_v1_stat(
    brewfs_client_t *client, const uint8_t *path, size_t path_len,
    brewfs_stat_v1 *output);
brewfs_status_t brewfs_v1_statfs(
    brewfs_client_t *client, brewfs_statfs_v1 *output);
brewfs_status_t brewfs_v1_mkdir(
    brewfs_client_t *client, const uint8_t *path, size_t path_len);
brewfs_status_t brewfs_v1_mkdir_p(
    brewfs_client_t *client, const uint8_t *path, size_t path_len);
brewfs_status_t brewfs_v1_delete(
    brewfs_client_t *client, const uint8_t *path, size_t path_len,
    uint8_t recursive);
brewfs_status_t brewfs_v1_rename(
    brewfs_client_t *client, const uint8_t *old_path, size_t old_len,
    const uint8_t *new_path, size_t new_len);
brewfs_status_t brewfs_v1_truncate(
    brewfs_client_t *client, const uint8_t *path, size_t path_len,
    uint64_t size);
 brewfs_status_t brewfs_v1_setattr(
    brewfs_client_t *client, const uint8_t *path, size_t path_len,
    const brewfs_setattr_v1 *input);

brewfs_status_t brewfs_v1_readdir_open(
    brewfs_client_t *client, const uint8_t *path, size_t path_len,
    brewfs_dir_t **out_dir);
brewfs_status_t brewfs_v1_readdir_next(
    brewfs_dir_t *dir, uint8_t *name, size_t name_capacity,
    size_t *required_name_length, uint64_t *inode, uint32_t *file_type,
    uint8_t *has_entry);
brewfs_status_t brewfs_v1_readdir_close(brewfs_dir_t *dir);

brewfs_status_t brewfs_v1_set_xattr(
    brewfs_client_t *client, const uint8_t *path, size_t path_len,
    const uint8_t *name, size_t name_len, const uint8_t *value,
    size_t value_len, uint32_t flags);
brewfs_status_t brewfs_v1_get_xattr(
    brewfs_client_t *client, const uint8_t *path, size_t path_len,
    const uint8_t *name, size_t name_len, uint8_t *value,
    size_t value_capacity, size_t *required_value_length);
brewfs_status_t brewfs_v1_list_xattr(
    brewfs_client_t *client, const uint8_t *path, size_t path_len,
    uint8_t *output, size_t output_capacity, size_t *required_output_length);
brewfs_status_t brewfs_v1_remove_xattr(
    brewfs_client_t *client, const uint8_t *path, size_t path_len,
    const uint8_t *name, size_t name_len);

uint64_t brewfs_v1_capabilities(void);
brewfs_status_t brewfs_v1_last_error(
    uint8_t *output, size_t capacity, size_t *required_length);

#ifdef __cplusplus
}
#endif

#endif /* BREWFS_H */
