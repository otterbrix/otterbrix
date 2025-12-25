import argparse
import os
import shutil
import sys

import xml.etree.ElementTree as ET

from common import sys_call, collect_files_by_ext

from termcolor import colored

clang_versions = [
  "clang-format-11",
  "clang-format-12",
  "clang-format-13",
  "/usr/local/opt/llvm/bin/clang-format", # llvm binaries installed by brew (keg only)
  "/opt/homebrew/bin/clang-format"
]

ext_to_format = [
    ".c",
    ".cc",
    ".cxx",
    ".cpp",
    ".h",
    ".hh",
    ".hxx",
    ".hpp",
    ".ii",
    ".ixx",
    ".ipp",
    ".inl",
    ".inc"
]

files_to_skip = [
]

class Clang:
  def __init__(self):
    self.clang = self.resolve_clang()

  def resolve_clang(self):
    # find existing version of clang
    clang_cmd = None
    for v in clang_versions:
      if shutil.which(v) is not None:
        clang_cmd = v
        break

    if not clang_cmd:
      print("Can't find clang binary")
      sys.exit(1)

    print(f"Found clang-format: {clang_cmd}")
    cmd = "{} --version ".format(clang_cmd)
    result, output = sys_call(cmd,
                      working_dir=os.getcwd(),
                      use_pipe=True)
    if result:
      s = output.decode("utf-8")
      print(s)

    return clang_cmd

  def collect_files(self, paths):
    files = []
    for d in paths:
      if not os.path.exists(d):
        continue
      if os.path.isdir(d):
        files.extend(collect_files_by_ext(d, ext_to_format, files_to_skip))
      elif os.path.splitext(d)[1] in ext_to_format:
        files.append(d)
    return files

  def format(self, search_paths):
    files = self.collect_files(search_paths)

    for f in files:
      print("Format {}".format(f))
      cmd = "{} -i {}".format(self.clang, f)
      sys_call(cmd, working_dir=os.getcwd())

  def check(self, search_paths) -> bool:
    files = self.collect_files(search_paths)

    final_result = True
    non_formatted_files = []

    for f in files:
      print(f'Look into {f}')
      print('Check {}'.format(f))
      cmd = "{} --output-replacements-xml {}".format(self.clang, f)
      result, output = sys_call(cmd,
                      working_dir=os.getcwd(),
                      use_pipe=True)

      if not result:
        final_result = False
        continue

      # check if there are any replacements
      s = output.decode("utf-8")
      if len(s) > 0:
        root = ET.fromstring(s)
        replacements = root.findall("replacement")

        if len(replacements) > 0:
          final_result = False
          non_formatted_files.append(f)

    return (final_result, non_formatted_files)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Call clang-format")
  parser.add_argument(
    "files",
    nargs="*",
    help=
    "List of input files or folders (relative to the working directory)",
  )
  parser.add_argument("--check", action="store_true")

  args = parser.parse_args()

  c = Clang()
  if args.check:
    passed, files = c.check(args.files)
    if not passed or len(files) > 0:
      print("Files that need formatting:")
      for f in files:
        print(colored(" {}".format(f), 'yellow'))

      if len(files) > 0:
        sys.exit(1)
      else:
        sys.exit(0)
  else:
    c.format(args.files)