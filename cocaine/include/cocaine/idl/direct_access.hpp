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

#include <elliptics/session.hpp>

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

	typedef deferred<std::string> result_type;
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

	typedef deferred<void> result_type;
      };

      struct lookup {
        typedef direct_access_tag tag;

        static const char* alias() {
	  return "lookup";
        }

        typedef boost::mpl::list<
	  dnet_id
	  > tuple_type;

	typedef deferred<void> result_type;
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

  }} // namespace cocaine::io

#endif
