# VGX Server

This project is used to build the:

- PyVGX pip package

## Prerequisites

TBD




### Build Python from source


mkdir ~/src
cd ~/src

curl -O https://www.python.org/ftp/python/3.13.5/Python-3.13.5.tgz
tar xzf Python-3.13.5.tgz
cd Python-3.13.5


./configure \
  --prefix=/usr/local \
  --enable-shared \
  --enable-optimizations \
  --with-openssl=$(pkg-config --variable=prefix openssl) \
  LDFLAGS='-Wl,-rpath=\$$ORIGIN/../lib'


make -j$(nproc)
sudo make altinstall


### Create and Activate Virtual Environment

cd ~
/usr/local/bin/python3.13 -m venv venv313
. venv313/bin/activate



### Install Build Tools

pip install --upgrade pip setuptools wheel build

sudo apt install ninja-build


### Build PyVGX

cd $VGXSERVER









## Building

TBD 

