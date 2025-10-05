# HCAS Filesystem Kernel Module Makefile

obj-m := hcasfs.o

# Multiple object files that make up the hcasfs module
hcasfs-objs := main.o super.o inode.o file_reg.o file_dir.o buffered_reader.o

# Kernel build directory - automatically detect running kernel
KDIR := /lib/modules/$(shell uname -r)/build

# Current directory
PWD := $(shell pwd)

# Default target
all:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

# Clean target
clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean

# Install module to system (requires root)
install: all
	$(MAKE) -C $(KDIR) M=$(PWD) modules_install
	depmod -A

# Uninstall module
uninstall:
	rm -f /lib/modules/$(shell uname -r)/extra/hcasfs.ko
	depmod -A

# Load module (for testing)
load:
	sudo insmod hcasfs.ko

# Unload module (for testing)  
unload:
	sudo rmmod hcasfs

# Show module info
info:
	modinfo hcasfs.ko

.PHONY: all clean install uninstall load unload info
