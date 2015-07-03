#ifndef __KINETICIO_FACTORY_HH__
#define	__KINETICIO_FACTORY_HH__

#include <memory>
#include "FileIoInterface.hh"
#include "FileAttrInterface.hh"

namespace IoFactory{
  std::shared_ptr<FileIoInterface> sharedFileIo();
  std::unique_ptr<FileIoInterface> uniqueFileIo();

  std::shared_ptr<FileAttrInterface> sharedFileAttr(const char* path);
  std::unique_ptr<FileAttrInterface> uniqueFileAttr(const char* path);
}

#endif	/* __KINETICIO_FACTORY_HH__ */

