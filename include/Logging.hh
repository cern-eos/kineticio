//------------------------------------------------------------------------------
//! @file Logging.hh
//! @author Paul Hermann Lensing
//! @brief Providing log functionality using arbitrary registered log functions
//------------------------------------------------------------------------------

/************************************************************************
 * KineticIo - a file io interface library to kinetic devices.          *
 *                                                                      *
 * This Source Code Form is subject to the terms of the Mozilla         *
 * Public License, v. 2.0. If a copy of the MPL was not                 *
 * distributed with this file, You can obtain one at                    *
 * https://mozilla.org/MP:/2.0/.                                        *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but is provided AS-IS, WITHOUT ANY WARRANTY; including without       *
 * the implied warranty of MERCHANTABILITY, NON-INFRINGEMENT or         *
 * FITNESS FOR A PARTICULAR PURPOSE. See the Mozilla Public             *
 * License for more details.                                            *
 ************************************************************************/

#ifndef KINETICIO_LOGGING_HH
#define KINETICIO_LOGGING_HH
#include "KineticIoFactory.hh"
#include "Utility.hh"
#include <mutex>
#include <sstream>
#include <syslog.h>

namespace kio {

  //----------------------------------------------------------------------------
  //! Accept variadic number of arguments to logging or to build a
  //! LoggingException. Registered log function will be called to do
  //! the actual logging outside the library.
  //----------------------------------------------------------------------------
  class Logger{
  public:
    //--------------------------------------------------------------------------
    //! Register supplied log function.
    //!
    //! @param lfunc the log function to use
    //! @param shouldfunc decide if log function should be called for a specific
    //!   log level.
    //--------------------------------------------------------------------------
    void registerLogFunction(logfunc_t lfunc, shouldlogfunc_t shouldfunc){
      std::lock_guard<std::mutex> lock(mutex);
      logFunction = lfunc;
      shouldLog = shouldfunc;
    }

    //--------------------------------------------------------------------------
    //! Log function accepting variadic arguments, log macros can be used
    //! for convenience instead of calling this function directly. If a call
    //! actually results in an output depends on set logfunc and shouldlogfunc.
    //!
    //! @param func the name of the func attempting to log
    //! @param file the name of the file containing the call to the log function
    //! @param line the line in the file containing the call to the log function
    //! @param level the log level as defined in syslog.h
    //! @param args an arbitrary number of variable type arguments to actually log
    //--------------------------------------------------------------------------
    template<typename...Args>
    void log(const char* func, const char* file, int line, int level, Args&&...args){
      std::lock_guard<std::mutex> lock(mutex);
      if(!logFunction || !shouldLog || !shouldLog(func,level))
        return;
      auto s = utility::Convert::toString(std::forward<Args>(args)...);
      logFunction(func,file,line,level,s.c_str());
    }

    //--------------------------------------------------------------------------
    //! Provide access to the static Logger instance.
    //! @return reference to the static Logger instance.
    //--------------------------------------------------------------------------
    static Logger& get(){
      static Logger l;
      return l;
    }

  private:
    //--------------------------------------------------------------------------
    //! Constructor. Private, access to Logger instance through get() method.
    //--------------------------------------------------------------------------
    explicit Logger(){};

  private:
    //! log function to use
    logfunc_t logFunction;
    //! function to test if log function should be called for a specific function name & log level
    shouldlogfunc_t shouldLog;
    //! concurrency
    std::mutex mutex;
  };
}

//! debug macro
#define kio_debug(message...)   kio::Logger::get().log(__FUNCTION__, __FILE__, __LINE__, LOG_DEBUG, message)
//! notice macro
#define kio_notice(message...)  kio::Logger::get().log(__FUNCTION__, __FILE__, __LINE__, LOG_NOTICE, message)
//! warning macro
#define kio_warning(message...) kio::Logger::get().log(__FUNCTION__, __FILE__, __LINE__, LOG_WARNING, message)
//! error macro
#define kio_error(message...) kio::Logger::get().log(__FUNCTION__, __FILE__, __LINE__, LOG_ERR, message)


#endif //KINETICIO_LOGGING_HH
