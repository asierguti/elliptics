/*
    Copyright (c) 2011-2014 Yandex
    Copyright (c) 2011-2014 Other contributors as noted in the AUTHORS file.

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

#ifndef COCAINE_DIRECT_ACCESS_API_HPP
#define COCAINE_DIRECT_ACCESS_API_HPP

#include "cocaine/common.hpp"
#include "cocaine/repository.hpp"
#include "cocaine/traits.hpp"
#include "json/json.h"
#include "elliptics/result_entry.hpp"

#include <memory>

using namespace ioremap::elliptics;

namespace cocaine {
  namespace api {

    struct direct_access_t {
      typedef direct_access_t category_type;

    virtual
    ~direct_access_t() {
      // Empty.
    }

      deferred<std::string> read_data(const dnet_id &id, uint64_t offset, uint64_t size);
      deferred<void> write_data(const dnet_id &id, const std::string &file, uint64_t remote_offset);          
      deferred<void> lookup(const dnet_id &id);

    };
  }} // namespace cocaine::api

#endif
