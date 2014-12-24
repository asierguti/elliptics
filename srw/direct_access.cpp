/*
 * 2014+ Copyright (c) Yandex
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */

#include <cocaine/include/cocaine/services/direct_access.hpp>
#include <cocaine/include/cocaine/api/direct_access.hpp>

#include <memory>
#include <iostream>
#include <fstream>


#include <cocaine/context.hpp>
#include <cocaine/logging.hpp>

#include <cocaine/traits/tuple.hpp>

#include <elliptics/interface.h>
#include <elliptics/srw.h>
#include <elliptics/utils.hpp>

#include "cocaine-json-trait.hpp"
//#include "dnet_id_trait.hpp"
#include <bindings/cpp/session_indexes.hpp>
#include "elliptics.h"

using namespace cocaine;
using namespace cocaine::io;
using namespace cocaine::service;
using namespace ioremap::elliptics;

using namespace std::placeholders;

struct dnet_node;

direct_access_t::direct_access_t(cocaine::context_t& context, cocaine::io::reactor_t& reactor, const std::string& name, const Json::Value& args, struct dnet_node* node):
  service_t(context, reactor, name, args), m_ctx (context), m_node (node)
{
  ioremap::elliptics::session sess (m_node);

  auto routes = sess.get_routes();
  std::set<int> groups_set;

  for(auto it = routes.begin(), end = routes.end(); it != end; ++it) {
    groups_set.insert(it->group_id);
  }

  m_groups = std::move(std::vector<int>(groups_set.begin(), groups_set.end()));

  on<cocaine::io::direct_access::read_data>("read_data", std::bind(&direct_access_t::read_data, this, _1, _2, _3));
  on<cocaine::io::direct_access::lookup>("lookup", std::bind(&direct_access_t::lookup, this, _1));
  on<cocaine::io::direct_access::write_data>("write_data", std::bind(&direct_access_t::write_data, this, _1, _2, _3));
}

deferred<std::string> direct_access_t::read_data(const dnet_id& key, uint64_t offset, uint64_t size)
{
  deferred<std::string> promise;

  ioremap::elliptics::key k(key);

  ioremap::elliptics::session sess (m_node);
  //  sess.set_exceptions_policy(0x00);
  sess.set_groups(m_groups);

  /*  ioremap::elliptics::key key_id = std::string ("test");
  key_id.transform(sess);
  dnet_id ke = key_id.id();

  ke.group_id = 1;
  */
  async_read_result r = sess.read_data (k, offset, size);
  r.connect(std::bind(&direct_access_t::on_read_completed, this, promise, _1, _2));

  return promise;
}

deferred<void> direct_access_t::write_data(const dnet_id &id, const std::string &file, uint64_t remote_offset)
{
  deferred<void> promise;

  ioremap::elliptics::key k(id);
  
  ioremap::elliptics::session sess (m_node);
  sess.set_exceptions_policy(0x00);
  sess.set_groups(m_groups);

  /*    ioremap::elliptics::key key_id = std::string ("test");
  key_id.transform(sess);
  dnet_id ke = key_id.id();

  ke.group_id = 1;
  */

  async_write_result r = sess.write_data (k, file, remote_offset);
  r.connect(std::bind(&direct_access_t::on_write_completed, this, promise, _1, _2));

  return promise;
}

deferred<void> direct_access_t::lookup(const dnet_id &id)
{
  deferred<void> promise;

  ioremap::elliptics::key k(id);

  ioremap::elliptics::session sess (m_node);
  sess.set_exceptions_policy(0x00);
  sess.set_groups(m_groups);

  auto r = sess.lookup (k);
  r.connect(std::bind(&direct_access_t::on_write_completed, this, promise, _1, _2));

  return promise;
}

void direct_access_t::on_read_completed(deferred<std::string> promise,
					const std::vector<ioremap::elliptics::read_result_entry> &result,
					const ioremap::elliptics::error_info &error)
{
  if (error) {
    promise.abort(-error.code(), error.message());
  } else {
    promise.write(result[0].file().to_string());
    }
}

void direct_access_t::on_write_completed(deferred<void> promise,
					 const std::vector<ioremap::elliptics::lookup_result_entry> &result,
					 const ioremap::elliptics::error_info &error)
{
  if (error) {
    promise.abort(error.code(), error.message());
  } else {
    promise.close();
    }
}
