//------------------------------------------------------------------------------
//! @file Logging.hh
//! @author Paul Hermann Lensing
//! @brief Providing log functionality using arbitrary registered log functions
//------------------------------------------------------------------------------
#ifndef KINETICIO_LOGGING_HH
#define KINETICIO_LOGGING_HH
#include "LoggingException.hh"
#include "KineticIoFactory.hh"
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
    //! for convinience instead of calling this function directly. If a call
    //! actually results in an output depends on set logfunc and shouldlogfunc.
    //!
    //! @param func the name of the func attempting to log
    //! @param file the name of the file containing the call to the log function
    //! @param line the line in the file containing the call to the log function
    //! @param level the log level as defined in syslog.h
    //! @param args an arbitray number of variable type arguments to actually log
    //--------------------------------------------------------------------------
    template<typename...Args>
    void log(const char* func, const char* file, int line, int level, Args&&...args){
      std::lock_guard<std::mutex> lock(mutex);
      if(!logFunction || !shouldLog || !shouldLog(func,level))
        return;
      std::stringstream message;
      argsToStream(message, std::forward<Args>(args)...);
      logFunction(func,file,line,level,message.str().c_str());
    }

    //--------------------------------------------------------------------------
    //! Generate a LoggingException accepting variadic arguments
    //!
    //! @param func the name of the func attempting to log
    //! @param file the name of the file containing the call to the log function
    //! @param line the line in the file containing the call to the log function
    //! @param errnum the error number that best represents the exception
    //! @param args an arbitray number of variable type arguments used to
    //!   generate the exception message
    //! @return the LoggingException constructed from the input paramters
    //--------------------------------------------------------------------------
    template<typename...Args>
    LoggingException exception(const char* func, const char* file, int line, int errnum, Args&&...args){
      std::stringstream message;
      argsToStream(message, std::forward<Args>(args)...);
      return LoggingException(errnum, func, file, line, message.str());
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

    //--------------------------------------------------------------------------
    //! Closure of recursive parsing function
    //! @param stream string stream to store Last parameter
    //! @param last the last argument to log
    //--------------------------------------------------------------------------
    template<typename Last>
    void argsToStream(std::stringstream& stream, Last&& last) {
      stream << last;
    }

    //--------------------------------------------------------------------------
    //! Recursive function to parse arbitrary number of variable type arguments
    //! @param stream string stream to store input parameters
    //! @param first the first of the arguments supplied to the log function
    //! @param rest the rest of the arguments should be stored in the log message
    //--------------------------------------------------------------------------
    template<typename First, typename...Rest >
    void argsToStream(std::stringstream& stream, First&& first, Rest&&...rest) {
      stream << first;
      argsToStream(stream, std::forward<Rest>(rest)...);
    }

  private:
    //! log function to use
    logfunc_t logFunction;
    //! function to test if log function should be called for a specific function name & log level
    shouldlogfunc_t shouldLog;
    //! concurrency
    std::mutex mutex;
  };
}

//! generate a LoggingException, errors are always thrown and can be logged by receiver
#define kio_exception(err, message...) kio::Logger::get().exception(__FUNCTION__, __FILE__, __LINE__, err, ## message)

//! debug macro
#define kio_debug(message...)   kio::Logger::get().log(__FUNCTION__, __FILE__, __LINE__, LOG_DEBUG, ## message)
//! notice macro
#define kio_notice(message...)  kio::Logger::get().log(__FUNCTION__, __FILE__, __LINE__, LOG_NOTICE, ## message)
//! warning macro
#define kio_warning(message...) kio::Logger::get().log(__FUNCTION__, __FILE__, __LINE__, LOG_WARNING, ## message)


#endif //KINETICIO_LOGGING_HH
