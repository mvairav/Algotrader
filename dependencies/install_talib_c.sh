#!/bin/bash 

wget https://github.com/TA-Lib/ta-lib/releases/download/v0.6.4/ta-lib-0.6.4-src.tar.gz
tar -xzf ta-lib-0.6.4-src.tar.gz
cd ta-lib-0.6.4
./configure
make
sudo make install

cd ..
rm -rf ta-lib-0.6.4

echo -e "\n\n\n"
echo "#========================================#"
echo "#                                        #"
echo "#  To uninstall => sudo make uninstall   #"
echo "#                                        #"
echo "#========================================#"




