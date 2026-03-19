#!/bin/bash

set -e

export http_proxy=http://10.74.176.8:11080
export https_proxy=http://10.74.176.8:11080
export no_proxy=localhost,127.0.0.1,localaddress,localdomain.com,internal,corp.kuaishou.com,test.gifshow.com,staging.kuaishou.com,kwaidc.com

# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs >rustup-init.sh
sh rustup-init.sh -y --default-toolchain none
export PATH="$PATH:$HOME/.cargo/bin"
rustup toolchain install

# disable some broken maven plugins
perl -0777 -pe 's/<!-- spotless -->.*?<\/plugin>//gs' -i pom.xml
perl -0777 -pe 's/<!-- scalafix -->.*?<\/plugin>//gs' -i pom.xml

# build auron package
mvn package -Pspark-241kwaiae -Prelease -Dmaven.test.skip
mvn package -Pspark-3.5 -Prelease -Dmaven.test.skip
