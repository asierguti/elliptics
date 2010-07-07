/*
 * 2009+ Copyright (c) Tuncer Ayaz <tuncer.ayaz@gmail.com>
 * 2009+ Copyright (c) Evgeniy Polyakov <zbr@ioremap.net>
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

#include "config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/wait.h>

#include <errno.h>
#include <ctype.h>
#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#ifndef __unused
#define __unused	__attribute__ ((unused))
#endif

#include "elliptics/packet.h"
#include "elliptics/interface.h"

#include "backends.h"

#include <tcadb.h>

struct tc_backend
{
	char	*env_dir;
	TCADB	*data, *hist;
};

static TCADB *tc_setup_db(struct tc_backend *be, struct dnet_io_attr *io)
{
	TCADB *db = be->data;

	if (io->flags & DNET_IO_FLAGS_HISTORY)
		db = be->hist;

	return db;
}

static int tc_get_data(void *state, struct tc_backend *be, struct dnet_cmd *cmd, struct dnet_attr *attr, void *buf)
{
	TCADB *db = be->data;
	int err;
	struct dnet_io_attr *io = buf;
	int size;
	void *ptr;
	struct dnet_data_req *r;

	buf += sizeof(struct dnet_io_attr);

	dnet_convert_io_attr(io);

	db = tc_setup_db(be, io);

	ptr = tcadbget(db, io->origin, DNET_ID_SIZE, &size);
	if (!ptr) {
		dnet_backend_log(DNET_LOG_ERROR, "%s: failed to read object.\n", dnet_dump_id(io->origin));
		err = -ENOENT;
		goto err_out_exit;
	}

	size = dnet_backend_check_get_size(io, size);

	dnet_backend_log(DNET_LOG_INFO,	"%s: read object: io_offset: %llu, io_size: %llu, io_flags: %x, size: %d.\n",
			dnet_dump_id(io->origin), (unsigned long long)io->offset,
			(unsigned long long)io->size, io->flags, size);

	if (!size) {
		err = 0;
		goto err_out_free;
	}

	if (attr->size == sizeof(struct dnet_io_attr)) {
		struct dnet_cmd *c;
		struct dnet_attr *a;
		struct dnet_io_attr *rio;

		r = dnet_req_alloc(state, sizeof(struct dnet_cmd) +
				sizeof(struct dnet_attr) + sizeof(struct dnet_io_attr));
		if (!r) {
			err = -ENOMEM;
			dnet_backend_log(DNET_LOG_ERROR, "%s: failed to allocate reply attributes.\n",
				dnet_dump_id(io->origin));
			goto err_out_free;
		}

		dnet_req_set_data(r, ptr, size, io->offset, 1);

		c = dnet_req_header(r);
		a = (struct dnet_attr *)(c + 1);
		rio = (struct dnet_io_attr *)(a + 1);

		memcpy(c->id, io->origin, DNET_ID_SIZE);
		memcpy(rio->origin, io->origin, DNET_ID_SIZE);

		dnet_backend_log(DNET_LOG_NOTICE, "%s: read reply offset: %llu, size: %d.\n",
			dnet_dump_id(io->origin), (unsigned long long)io->offset, size);

		if (cmd->flags & DNET_FLAGS_NEED_ACK)
			c->flags = DNET_FLAGS_MORE;

		c->status = 0;
		c->size = sizeof(struct dnet_attr) + sizeof(struct dnet_io_attr) + size;
		c->trans = cmd->trans | DNET_TRANS_REPLY;

		a->cmd = DNET_CMD_READ;
		a->size = sizeof(struct dnet_io_attr) + size;
		a->flags = attr->flags;

		rio->size = size;
		rio->offset = io->offset;
		rio->flags = io->flags;

		dnet_convert_cmd(c);
		dnet_convert_attr(a);
		dnet_convert_io_attr(rio);

		err = dnet_data_ready(state, r);
		if (err)
			goto err_out_free_req;
	} else {
		if ((unsigned)size > attr->size - sizeof(struct dnet_io_attr))
			size = attr->size - sizeof(struct dnet_io_attr);
		memcpy(buf, ptr + io->offset, size);

		io->size = size;
		attr->size = sizeof(struct dnet_io_attr) + io->size;
	}

	return 0;

err_out_free_req:
	dnet_req_destroy(r, err);
	return err;

err_out_free:
	free(ptr);
err_out_exit:
	return err;
}

static int tc_write_history_meta(void *state, void *backend, struct dnet_io_attr *io,
		struct dnet_meta *m, void *data)
{
	TCADB *db = backend;
	void *hdata, *new_hdata;
	int size, err;
	bool res;
	
	hdata = tcadbget(db, io->origin, DNET_ID_SIZE, &size);
	if (!hdata) {
		err = -ENOMEM;
		dnet_backend_log(DNET_LOG_ERROR, "%s: failed to read history object.\n", dnet_dump_id(io->id));
		goto err_out_exit;
	}

	new_hdata = backend_process_meta(state, io, hdata, (uint32_t *)&size, m, data);
	if (!new_hdata) {
		err = -ENOMEM;
		dnet_backend_log(DNET_LOG_ERROR, "%s: failed to update history object.\n", dnet_dump_id(io->id));
		goto err_out_free;
	}
	hdata = new_hdata;

	res = tcadbput(db, io->origin, DNET_ID_SIZE, hdata, size);
	if (!res) {
		err = -EINVAL;
		dnet_backend_log(DNET_LOG_ERROR, "%s: failed to write history object.\n", dnet_dump_id(io->id));
		goto err_out_free;
	}

	err = 0;

err_out_free:
	free(hdata);
err_out_exit:
	return err;
}

static int tc_write_history(TCADB *db, void *state, struct dnet_io_attr *io, void *iodata)
{
	return backend_write_history(state, db, io, iodata, tc_write_history_meta);
}

static int tc_put_data(void *state, struct tc_backend *be, struct dnet_cmd *cmd, void *data)
{
	int err;
	TCADB *db = be->data;
	struct dnet_io_attr *io = data;
	struct dnet_history_entry *e, n;
	bool res = true;

	dnet_convert_io_attr(io);

	data += sizeof(struct dnet_io_attr);

	db = tc_setup_db(be, io);

	res = tcadbtranbegin(db);
	if (!res) {
		dnet_backend_log(DNET_LOG_ERROR, "%s: failed to start write transaction.\n", dnet_dump_id(cmd->id));
		err = -EINVAL;
		goto err_out_exit;
	}

	if (io->flags & DNET_IO_FLAGS_HISTORY) {
		err = tc_write_history(be->hist, state, io, io + 1);
		if (err)
			goto err_out_data_trans_abort;
	} else {
		if (io->flags & DNET_IO_FLAGS_APPEND)
			res = tcadbputcat(db, io->origin, DNET_ID_SIZE, data, io->size);
		else
			res = tcadbput(db, io->origin, DNET_ID_SIZE, data, io->size);
		if (!res) {
			dnet_backend_log(DNET_LOG_ERROR, "%s: direct object put failed: offset: %llu, size: %llu.\n",
				dnet_dump_id(io->origin), (unsigned long long)io->offset,
				(unsigned long long)io->size);
			err = -EINVAL;
			goto err_out_data_trans_abort;
		}

		dnet_backend_log(DNET_LOG_NOTICE, "%s: stored %s object: size: %llu, offset: %llu.\n",
				dnet_dump_id(io->origin),
				(io->flags & DNET_IO_FLAGS_HISTORY) ? "history" : "data",
				(unsigned long long)io->size, (unsigned long long)io->offset);

		if (!(io->flags & DNET_IO_FLAGS_NO_HISTORY_UPDATE)) {
			e = &n;

			dnet_setup_history_entry(e, io->id, io->size, io->offset, NULL, io->flags);

			io->flags |= DNET_IO_FLAGS_APPEND | DNET_IO_FLAGS_HISTORY;
			io->flags &= ~DNET_IO_FLAGS_META;
			io->size = sizeof(struct dnet_history_entry);
			io->offset = 0;

			err = tc_write_history(be->hist, state, io, &e);
			if (err)
				goto err_out_data_trans_abort;
		}
	}

	res = tcadbtrancommit(db);
	if (!res) {
		dnet_backend_log(DNET_LOG_ERROR, "%s: failed to commit write transaction.\n",
			dnet_dump_id(io->origin));
		err = -EINVAL;
		goto err_out_data_trans_abort;
	}

	return 0;

err_out_data_trans_abort:
	tcadbtranabort(db);
err_out_exit:
	return err;
}

static int tc_get_flags(struct tc_backend *be, const unsigned char *id, uint32_t *flags)
{
	int size;
	struct dnet_history_entry *e;

	e = tcadbget(be->hist, id, DNET_ID_SIZE, &size);
	if (!e) {
		dnet_backend_log(DNET_LOG_ERROR, "Failed to get history for id: %s.\n", dnet_dump_id_len(id, DNET_ID_SIZE));
		return -EINVAL;
	}

	*flags = dnet_bswap32(e->flags);
	free(e);
	return 0;
}

static int tc_list_raw(void *state, struct tc_backend *be, struct dnet_cmd *cmd,
		struct dnet_attr *attr)
{
	int err = 0, num, size, i;
	int out = attr->flags & DNET_ATTR_ID_OUT;
	TCADB *e = be->hist;
	uint32_t flags;
	unsigned char id[DNET_ID_SIZE], start, last;
	TCLIST *l;
	int inum = 10240, ipos = 0, wrap = 0;
	struct dnet_id ids[inum];

	if (out)
		dnet_state_get_next_id(state, id);

	last = id[0] - 1;

	if (cmd->id[0] == last)
		wrap = 1;

	for (start = cmd->id[0]; start != last || wrap; --start) {
		wrap = 0;

		l = tcadbfwmkeys(e, &start, 1, -1);
		if (!l)
			continue;

		num = tclistnum(l);
		if (!num)
			goto out_clean;

		dnet_backend_log(DNET_LOG_INFO, "%02x: %d object(s).\n", start, num);

		for (i=0; i<num; ++i) {
			const unsigned char *idx = tclistval(l, i, &size);

			if (!idx)
				break;

			if (attr->flags & DNET_ATTR_ID_OUT) {
				if (!dnet_id_within_range(idx, id, cmd->id))
					continue;
			}

			if (ipos == inum) {
				err = dnet_send_reply(state, cmd, attr, ids, ipos * sizeof(struct dnet_id), 1);
				if (err)
					goto out_clean;

				ipos = 0;
			}

			flags = 0;
			if (attr->flags & DNET_ATTR_ID_FLAGS) {
				err = tc_get_flags(be, idx, &flags);
				if (err)
					continue;
			}

			dnet_backend_log(DNET_LOG_INFO, "%s\n", dnet_dump_id(idx));
			memcpy(ids[ipos].id, idx, DNET_ID_SIZE);
			ids[ipos].flags = flags;

			dnet_convert_id(&ids[ipos]);

			ipos++;
		}

out_clean:
		tclistdel(l);
		if (err)
			goto err_out_exit;
	}

	if (ipos) {
		err = dnet_send_reply(state, cmd, attr, ids, ipos * DNET_ID_SIZE, 0);
		if (err)
			goto err_out_exit;
	}

	return 0;

err_out_exit:
	return err;
}

static int tc_list(void *state, struct tc_backend *be, struct dnet_cmd *cmd,
		struct dnet_attr *attr)
{
	return tc_list_raw(state, be, cmd, attr);
}


static int tc_del(void *state __unused, struct tc_backend *be, struct dnet_cmd *cmd,
		struct dnet_attr *attr, void *buf)
{
	int err = -EINVAL;

	if (!attr || !buf)
		goto err_out_exit;

	tcadbout(be->hist, cmd->id, DNET_ID_SIZE);
	tcadbout(be->data, cmd->id, DNET_ID_SIZE);
	return 0;

err_out_exit:
	return err;
}

static int tc_backend_command_handler(void *state, void *priv,
		struct dnet_cmd *cmd, struct dnet_attr *attr,
		void *data)
{
	int err;
	struct tc_backend *e = priv;

	switch (attr->cmd) {
		case DNET_CMD_WRITE:
			err = tc_put_data(state, e, cmd, data);
			break;
		case DNET_CMD_READ:
			err = tc_get_data(state, e, cmd, attr, data);
			break;
		case DNET_CMD_LIST:
			err = tc_list(state, e, cmd, attr);
			break;
		case DNET_CMD_STAT:
			err = backend_stat(state, e->env_dir, cmd, attr);
			break;
		case DNET_CMD_DEL:
			err = tc_del(state, e, cmd, attr, data);
			break;
		default:
			err = -EINVAL;
			break;
	}

	return err;
}

static void tc_backend_close(TCADB *db)
{
	tcadbclose(db);
	tcadbdel(db);
}

static int tc_backend_open(TCADB *adb, const char *env_dir, const char *file)
{
	int err;
	char *path;
	size_t len;

	if (!env_dir) {
		if (!tcadbopen(adb, file)) {
			err = -EINVAL;
			goto err_out_print;
		}

		return 0;
	}

	/* if env_dir passed open db there
	 *
	 * Create path string from env_dir and file
	 * Added place for '/' and null byte at the end.
	 */
	len = strlen(env_dir) + strlen(file) + 2;

	path = malloc(len);
	if (!path) {
		err = -ENOMEM;
		dnet_backend_log(DNET_LOG_ERROR, "tc: failed to allocate %zu bytes for path '%s/%s'.\n", len, env_dir, file);
		goto err_out_exit;
	}

	snprintf(path, len, "%s/%s", env_dir, file);

	/* try to open database in env_dir */
	if (!tcadbopen(adb, path)) {
		err = -EINVAL;
		goto err_out_free;
	}

	free(path);

	return 0;

err_out_free:
	free(path);
err_out_print:
	dnet_backend_log(DNET_LOG_ERROR, "Failed to open database at dir: '%s', file: '%s'.\n", env_dir, file);
err_out_exit:
	return err;
}

static TCADB *tc_backend_create(const char *env_dir, const char *name)
{
	TCADB *db;
	int err;

	db = tcadbnew();
	if (!db) {
		dnet_backend_log(DNET_LOG_ERROR, "Failed to create new TC database '%s' in '%s'.\n", name, env_dir);
		goto err_out_exit;
	}

	err = tc_backend_open(db, env_dir, name);
	if (err) {
		dnet_backend_log(DNET_LOG_ERROR, "Failed to open new TC database '%s' in '%s': %d.\n", name, env_dir, err);
		goto err_out_close;
	}

	return db;

err_out_close:
	tcadbclose(db);
err_out_exit:
	return NULL;
}

static int dnet_tc_config_init(struct dnet_config_backend *b, struct dnet_config *c)
{
	c->command_private = b->data;
	c->command_handler = tc_backend_command_handler;

	return 0;
}

static void dnet_tc_config_cleanup(struct dnet_config_backend *b)
{
	struct tc_backend *be = b->data;

	tc_backend_close(be->data);
	tc_backend_close(be->hist);
	free(be->env_dir);
}

static int dnet_tc_set_env(struct dnet_config_backend *b, char *key __unused, char *root)
{
	struct tc_backend *be = b->data;

	be->env_dir = strdup(root);
	if (!be->env_dir)
		return -ENOMEM;

	return 0;
}

static int dnet_tc_set_data(struct dnet_config_backend *b, char *key __unused, char *file)
{
	struct tc_backend *be = b->data;

	be->data = tc_backend_create(be->env_dir, file);
	if (!be->data)
		return -EINVAL;

	return 0;
}

static int dnet_tc_set_history(struct dnet_config_backend *b, char *key __unused, char *file)
{
	struct tc_backend *be = b->data;

	be->hist = tc_backend_create(be->env_dir, file);
	if (!be->hist)
		return -EINVAL;

	return 0;
}

static struct dnet_config_entry dnet_cfg_entries_tc[] = {
	{"environment_dir", dnet_tc_set_env},
	{"database_file", dnet_tc_set_data},
	{"history_file", dnet_tc_set_history},
};

static struct dnet_config_backend dnet_tc_backend = {
	.name			= "tokyocabinet",
	.ent			= dnet_cfg_entries_tc,
	.num			= ARRAY_SIZE(dnet_cfg_entries_tc),
	.size			= sizeof(struct tc_backend),
	.init			= dnet_tc_config_init,
	.cleanup		= dnet_tc_config_cleanup,
};

int dnet_tc_backend_init()
{
	return dnet_backend_register(&dnet_tc_backend);
}

void dnet_tc_backend_exit(void)
{
	/* cleanup routing will be called explicitly through backend->cleanup() callback */
}

