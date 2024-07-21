# Using the dissector

The dissector can be used with `wireshark` and `tshark`. It requires another wireshark module [`luagcrypt`](https://github.com/Lekensteyn/luagcrypt) which is not packaged in most Linux distributions and needs building from source. Instructions here are suitable for Debian and its derivatives (tested on Debian 12 Bookworm amd64).

## Build `luagcrypt.so`
```
sudo apt install lua5.2 liblua5.2-dev libgcrypt20-dev libgpg-error-dev
git clone https://github.com/Lekensteyn/luagcrypt.git
cd luagcrypt
make LUA_DIR=/usr
```
## Install `luagcrypt.so`
The shared object library should be copied to `/usr/local/lib/lua/5.2/`
```
mkdir --parents /usr/local/lib/lua/5.2/
cp luagcrypt.so /usr/local/lib/lua/5.2/
```
Additionally, the system where the dissector is used needs these packages installing (if not already present): `libgcrypt20 libgpg-error0`
## Install `baichuan.lua`
Copy the dissector to the host where `wireshark` (or `tshark`) will be used to analyse the captured packets:
```
mkdir --parents $HOME/.local/lib/wireshark/plugins/
cp baichuan.lua $HOME/.local/lib/wireshark/plugins/
```

