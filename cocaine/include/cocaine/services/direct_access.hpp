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

#ifndef COCAINE_DIRECT_ACCESS_SERVICE_INTERFACE_HPP
#define COCAINE_DIRECT_ACCESS_SERVICE_INTERFACE_HPP

#include <cocaine/rpc/protocol.hpp>
#include <cocaine/api/service.hpp>
#include <cocaine/include/cocaine/idl/direct_access.hpp>
#include <elliptics/session.hpp>
#include <cocaine/rpc/slots/deferred.hpp>

#include "cocaine/common.hpp"
#include "cocaine/traits.hpp"

#include <boost/asio.hpp>

using namespace ioremap::elliptics;

namespace cocaine { namespace io {

    struct direct_access_tag;

    typedef int64_t timer_id_t;

    struct direct_access {
      struct read_data {
        typedef direct_access_tag tag;

        static const char* alias() {
	  return "read_data";
        }

        typedef boost::mpl::list<
	  dnet_id,
	  uint64_t,
	  uint64_t
	  > tuple_type;

	typedef std::string result_type;
      };

      struct write_data {
        typedef direct_access_tag tag;

        static const char* alias() {
	  return "write_data";
        }

        typedef boost::mpl::list<
	  dnet_id,
	  std::string,
	  uint64_t
	  > tuple_type;

	typedef dnet_async_service_result result_type;
      };

      struct lookup {
        typedef direct_access_tag tag;

        static const char* alias() {
	  return "lookup";
        }

        typedef boost::mpl::list<
	  dnet_id
	  > tuple_type;

	typedef dnet_async_service_result result_type;
      };
    };

    template<>
    struct protocol<direct_access_tag> {
      typedef boost::mpl::int_<
        1
	>::type version;

      typedef mpl::list<
        direct_access::read_data,
	direct_access::write_data,
	direct_access::lookup
	> type;
    };

  }

  namespace service {

    class direct_access_t:
      public api::service_t
    {
    public:
      direct_access_t(cocaine::context_t& context, cocaine::io::reactor_t& reactor, const std::string& name, const Json::Value& args,  struct dnet_node* node);

    private:

      deferred<std::string> read_data(const dnet_id &key, uint64_t offset, uint64_t size);
      deferred<dnet_async_service_result> write_data(const dnet_id &id, const std::string &file, uint64_t remote_offset);
      deferred<dnet_async_service_result> lookup(const dnet_id &id);

      void on_read_completed(deferred<std::string> promise,
			     const std::vector<ioremap::elliptics::read_result_entry> &result,
			     const ioremap::elliptics::error_info &error);

      void on_write_completed(deferred<dnet_async_service_result> promise,
			const std::vector<ioremap::elliptics::lookup_result_entry> &result,
			const ioremap::elliptics::error_info &error);

    private:
      boost::asio::io_service m_queue;

      ioremap::elliptics::node m_no;

      const std::unique_ptr<logging::log_t> m_log;

      context_t& m_ctx;
      dnet_node *m_node;

      std::vector<int> m_groups;
    };

  }} // namespace cocaine::io

#endif
