/*
Copyright (c) 2013 Andrey Goryachev <andrey.goryachev@gmail.com>
Copyright (c) 2013 Andrey Sibiryov <me@kobology.ru>
Copyright (c) 2011-2013 Other contributors as noted in the AUTHORS file.

This file is part of Cocaine.

Cocaine is free software; you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

Cocaine is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef COCAINE_FRAMEWORK_SERVICES_DIRECT_ACCESS_HPP
#define COCAINE_FRAMEWORK_SERVICES_DIRECT_ACCESS_HPP

#include <cocaine/services/direct_access.hpp>
#include <cocaine/framework/service.hpp>
#include <bindings/cpp/session_indexes.hpp>
#include <elliptics/packet.h>


namespace cocaine { namespace framework {

struct direct_access_service_t :
    public service_t
{
    static const unsigned int version = cocaine::io::protocol<cocaine::io::direct_access_tag>::version::value;

    direct_access_service_t(std::shared_ptr<service_connection_t> connection) :
        service_t(connection)
    {
        // pass
    }

    service_traits<cocaine::io::direct_access::read_data>::future_type
    read_data(const dnet_id& key,
	      uint64_t offset,
	      uint64_t size)
    {
      return call<cocaine::io::direct_access::read_data>(key, offset, size);
    }

    service_traits<cocaine::io::direct_access::write_data>::future_type
    write_data(const dnet_id& id,
          const std::string& file,
          uint64_t remote_offset)
    {
        return call<cocaine::io::direct_access::write_data>(id, file, remote_offset);
    }

    service_traits<cocaine::io::direct_access::lookup>::future_type
    lookup(const dnet_id& id)
    {
        return call<cocaine::io::direct_access::lookup>(id);
    }
};

}} // namespace cocaine::framework

#endif // COCAINE_FRAMEWORK_SERVICES_STORAGE_HPP
