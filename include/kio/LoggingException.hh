//------------------------------------------------------------------------------
//! @file LoggingException.hh
//! @author Paul Hermann Lensing
//! @brief Providing information to enable detailed logging.
//------------------------------------------------------------------------------
#ifndef __KINETICIO_LOGGINGEXCEPTION_HH__
#define	__KINETICIO_LOGGINGEXCEPTION_HH__

#include <exception>
#include <string>

//------------------------------------------------------------------------------
//! Providing information to enable detailed logging.
//------------------------------------------------------------------------------
class LoggingException : public std::exception {
public:
  //----------------------------------------------------------------------------
  //! Get standard error code that best fits the thrown exception
  //
  //! @return the standard error code that best fits the thrown exception
  //----------------------------------------------------------------------------
  int errnum() const {return _errnum;}

  //----------------------------------------------------------------------------
  //! Get the function name that threw the exception
  //
  //! @return function name that threw the exception
  //----------------------------------------------------------------------------
  const char* function() const {return _function.c_str();}

  //----------------------------------------------------------------------------
  //! Get the file name of the function that threw the exception
  //
  //! @return file name of the function that threw the exception
  //----------------------------------------------------------------------------
  const char* file() const {return _file.c_str();}

  //----------------------------------------------------------------------------
  //! Get the line number where the exception was created
  //
  //! @return line number where the exception was created
  //----------------------------------------------------------------------------
  int line() const {return _line;}

  //----------------------------------------------------------------------------
  //! Get the error message
  //
  //! @return the error message
  //----------------------------------------------------------------------------
  const char* what() const throw() {return _message.c_str();}

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  LoggingException(int errnum, std::string function, std::string file,
          int line, std::string  message) :
          _errnum(errnum), _function(std::move(function)), _file(std::move(file)),
          _line(line), _message(std::move(message))
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~LoggingException() throw()
  {}

private:
  //! the standard error code that best fits the thrown exception
  int _errnum;
  //! the function name that threw the exception
  std::string _function;
  //! the file the function that threw the exception is located in
  std::string _file;
  //! the line in the file that threw the exception
  int _line;
  //! the exception message
  std::string _message;
};

#endif	/* __KINETICIO_LOGGINGEXCEPTION_HH__ */

