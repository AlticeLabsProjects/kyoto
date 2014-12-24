#!/bin/bash -e
#
# Example build and install procedure.
#

if [ -n "$1" ]; then
	KYOTO_ROOT="$1"
else
	KYOTO_ROOT="/usr/local"
fi


echo "Building Kyoto Cabinet..."
pushd kyotocabinet >/dev/null
if [ -f Makefile ]; then
	make distclean
fi

./configure --prefix="${KYOTO_ROOT}"
make -j2
popd >/dev/null


echo
echo "Building Kyoto Tycoon..."
pushd kyototycoon >/dev/null
if [ -f Makefile ]; then
	make distclean
fi

PKG_CONFIG_PATH="../kyotocabinet" \
CPPFLAGS="-I../kyotocabinet" \
LDFLAGS="-L../kyotocabinet" \
./configure --prefix="${KYOTO_ROOT}" --with-kc="${KYOTO_ROOT}" --enable-lua
make -j2
popd >/dev/null

echo
echo "Installing into \"$KYOTO_ROOT\"..."

pushd kyotocabinet > /dev/null
make install
popd >/dev/null

pushd kyototycoon > /dev/null
make install
popd >/dev/null

if ! echo "$KYOTO_ROOT" | egrep -q "/usr(/local)?"; then
	echo
	echo "Installing in a non-system directory requires declaring \"$KYOTO_ROOT/lib\" to the system linker..."
	if [ -w "/etc/ld.so.conf.d" ]; then
		echo "$KYOTO_ROOT/lib" > /etc/ld.so.conf.d/kyoto.conf
		/sbin/ldconfig
	else
		echo "ERROR: Must be done manually. Please run these two commands as \"root\":"
		echo
		echo "  echo \"$KYOTO_ROOT/lib\" > /etc/ld.so.conf.d/kyoto.conf"
		echo "  /sbin/ldconfig"
	fi
fi

echo
echo "All done!"


# EOF - install.sh
