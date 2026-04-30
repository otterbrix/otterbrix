FROM mcr.microsoft.com/devcontainers/cpp:1-ubuntu22.04

RUN apt update && \
    apt upgrade -y && \
    apt install -y \
        build-essential \
        ninja-build \
        python3-pip \
        python3-venv \
        python3-dev curl gnupg apt-transport-https \
        zlib1g \
        flex \
        bison && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

RUN    pip3 install --no-cache-dir conan==2.20.0 pytest==6.2.5 cmake && \
       conan profile detect --force && \
       conan remote add otterbrix http://conan.otterbrix.com

# [Optional] Uncomment this section to install additional vcpkg ports.
# RUN su vscode -c "${VCPKG_ROOT}/vcpkg install <your-port-name-here>"

# [Optional] Uncomment this section to install additional packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>
