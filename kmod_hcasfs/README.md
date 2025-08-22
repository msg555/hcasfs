# HCAS Filesystem Kernel Module

This is a Linux kernel module that implements hcasfs.

## Building

```bash
# Build the module
make

# Clean build artifacts  
make clean
```

## Testing

```bash
# Load the module
make load

# Check if loaded
lsmod | grep hcasfs

# View module info
make info

# Check kernel messages
dmesg | tail

# Unload the module
make unload
```
