#ifndef KINETICIO_LOGGING_HH
#define KINETICIO_LOGGING_HH
#include "LoggingException.hh"
#include "KineticIoFactory.hh"
#include <mutex>
#include <syslog.h>

namespace kio {

  class Logger{

  public:
    void registerLogFunction(logfunc_t lfunc, shouldlogfunc_t shouldfunc){
      std::lock_guard<std::mutex> lock(mutex);
      logFunction = lfunc;
      shouldLog = shouldfunc;
    }

    template<typename...Args>
    void log(const char* func, const char* file, int line, int level, Args&&...args){
      std::lock_guard<std::mutex> lock(mutex);
      if(!logFunction || !shouldLog || !shouldLog(func,level))
        return;
      std::stringstream message;
      argsToStream(message, std::forward<Args>(args)...);
      logFunction(func,file,line,level,message.str().c_str());
    }

    template<typename...Args>
    LoggingException exception(const char* func, const char* file, int line, int errnum, Args&&...args){
      std::stringstream message;
      argsToStream(message, std::forward<Args>(args)...);
      return LoggingException(errnum, func, file, line, message.str());
    }

    static Logger& get(){
      static Logger l;
      return l;
    }
   private:
    explicit Logger(){};

    template<typename Last>
    void argsToStream(std::stringstream& stream, Last&& last) {
      stream << last;
    }

    template<typename First, typename...Rest >
    void argsToStream(std::stringstream& stream, First&& parm1, Rest&&...parm) {
      stream << parm1;
      argsToStream(stream, std::forward<Rest>(parm)...);
    }

  private:
    logfunc_t logFunction;
    shouldlogfunc_t shouldLog;
    std::mutex mutex;
  };
}


#define kio_exception(err, message...) kio::Logger::get().exception(__FUNCTION__, __FILE__, __LINE__, err, ## message)
#define kio_debug(message...)   kio::Logger::get().log(__FUNCTION__, __FILE__, __LINE__, LOG_DEBUG, ## message)
#define kio_notice(message...)  kio::Logger::get().log(__FUNCTION__, __FILE__, __LINE__, LOG_NOTICE, ## message)
#define kio_warning(message...) kio::Logger::get().log(__FUNCTION__, __FILE__, __LINE__, LOG_WARNING, ## message)


#endif //KINETICIO_LOGGING_HH
