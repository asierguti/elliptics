/*
    Copyright (c) 2011-2014 Asier Gutierrez <asierguti@gmail.com>
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

//#include "cocaine/locked_ptr.hpp"
#include "cocaine/repository.hpp"

#include "cocaine/traits.hpp"
#include "json/json.h"

#include "elliptics/result_entry.hpp"

#include <mutex>
#include <sstream>

#include <iostream>
#include <fstream>
#include <memory>

using namespace ioremap::elliptics;

namespace cocaine {
  /*
  struct storage_error_t:
    public error_t
  {
    template<typename... Args>
    storage_error_t(const std::string& format, const Args&... args):
      error_t(format, args...)
    { }
    };*/

  namespace api {

    struct direct_access_t {
      typedef direct_access_t category_type;

    virtual
    ~direct_access_t() {
      // Empty.
    }

      deferred<std::string> read_data(const std::string &id, const std::vector<int> &groups, uint64_t offset, uint64_t size);
      deferred<void> write_data(const std::string &id, const std::string &file, uint64_t remote_offset);          
      deferred<void> lookup(const std::string &id);

    };


    /* template<>
       struct category_traits<direct_access_t> {
      typedef std::shared_ptr<direct_access_t> ptr_type;

      struct factory_type: public basic_factory<direct_access_t> {
        virtual
        ptr_type
        get(context_t& context, const std::string& name, const Json::Value& args) = 0;
      };

    template<class T>
    struct default_factory: public factory_type {
        virtual
        ptr_type
        get(context_t& context, const std::string& name, const Json::Value& args) {
            std::lock_guard<std::mutex> guard(m_mutex);

  std::ofstream myfile;
  myfile.open ("/var/log/factory.log");
  myfile << "Writing this to a file.\n";
  myfile.close();

	    std::cerr << "Factory";

            typename instance_map_t::iterator it(m_instances.find(name));

            ptr_type instance;

            if(it != m_instances.end()) {
                instance = it->second.lock();
            }

            if(!instance) {
                instance = std::make_shared<T>(
                    std::ref(context),
                    name,
                    args
                );

                m_instances[name] = instance;
            }

            return instance;
        }

    private:
        typedef std::map<
            std::string,
            std::weak_ptr<direct_access_t>
        > instance_map_t;

        instance_map_t m_instances;
        std::mutex m_mutex;
    };

    };

    category_traits<direct_access_t>::ptr_type
    direct_access(context_t& context, const std::string& name);
    */
  }} // namespace cocaine::api

#endif
