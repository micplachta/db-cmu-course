#include <buffer/buffer_pool_manager.hpp>
#include <config.hpp>
#include <storage/disk_manager.hpp>
#include <storage/page_guard.hpp>

#include <cstring>
#include <filesystem>
#include <iostream>

static std::filesystem::path file_name("test.db");
const size_t FRAMES = 10;

int main(int argc, char** argv) {
  auto disk_manager = std::make_shared<DiskManager>(file_name);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const PageId_t page_id = bpm->NewPage();
  const std::string str = "Hello, world!";

  {
    auto guard = bpm->WritePage(page_id);
    memcpy(guard.GetDataMut(), str.c_str(), str.length() + 1);

    if (strncmp(guard.GetData(), str.c_str(), str.size())) {
      std::cerr << __LINE__ << ". expected " << guard.GetData()
                << " == " << str.c_str() << "\n";
      return -1;
    }
  }

  {
    const auto guard = bpm->ReadPage(page_id);
    if (strncmp(guard.GetData(), str.c_str(), str.size())) {
      std::cerr << __LINE__ << ". expected " << guard.GetData()
                << " == " << str.c_str() << "\n";
      return -1;
    }
  }

  {
    const auto guard = bpm->ReadPage(page_id);
    if (strncmp(guard.GetData(), str.c_str(), str.size())) {
      std::cerr << __LINE__ << ". expected " << guard.GetData()
                << " == " << str.c_str() << "\n";
      return -1;
    }
  }

  if (!bpm->DeletePage(page_id)) {
    std::cerr << __LINE__ << ". failed to delete page\n";
  }
}
