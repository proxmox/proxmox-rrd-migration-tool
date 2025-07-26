include /usr/share/dpkg/default.mk

PACKAGE="proxmox-rrd-migration-tool"
CRATENAME="proxmox-rrd-migration-tool"

BUILDDIR ?= $(PACKAGE)-$(DEB_VERSION_UPSTREAM)
ORIG_SRC_TAR=$(PACKAGE)_$(DEB_VERSION_UPSTREAM).orig.tar.gz

DEB=$(PACKAGE)_$(DEB_VERSION)_$(DEB_HOST_ARCH).deb
DBG_DEB=$(PACKAGE)-dbgsym_$(DEB_VERSION)_$(DEB_HOST_ARCH).deb
DSC=$(PACKAGE)_$(DEB_VERSION).dsc

CARGO ?= cargo
ifeq ($(BUILD_MODE), release)
CARGO_BUILD_ARGS += --release
COMPILEDIR := target/release
else
COMPILEDIR := target/debug
endif

PREFIX = /usr
LIBEXECDIR = $(PREFIX)/libexec
PROXMOX_LIBEXECDIR = $(LIBEXECDIR)/proxmox

PROXMOX_RRD_MIGRATION_TOOL_BIN := $(addprefix $(COMPILEDIR)/,proxmox-rrd-migration-tool)

all:

install: $(PROXMOX_RRD_MIGRATION_TOOL_BIN)
	install -dm755 $(DESTDIR)$(PROXMOX_LIBEXECDIR)
	install -m755 $(PROXMOX_RRD_MIGRATION_TOOL_BIN) $(DESTDIR)$(PROXMOX_LIBEXECDIR)/

$(PROXMOX_RRD_MIGRATION_TOOL_BIN): .do-cargo-build
.do-cargo-build:
	$(CARGO) build $(CARGO_BUILD_ARGS)
	touch .do-cargo-build


.PHONY: cargo-build
cargo-build: .do-cargo-build

$(BUILDDIR):
	rm -rf $@ $@.tmp
	mkdir $@.tmp
	cp -a debian/ src/ Makefile Cargo.toml wrapper.h build.rs $@.tmp
	echo "git clone git://git.proxmox.com/git/proxmox-rrd-migration-tool.git\\ngit checkout $$(git rev-parse HEAD)" \
	    > $@.tmp/debian/SOURCE
	mv $@.tmp $@


$(ORIG_SRC_TAR): $(BUILDDIR)
	tar czf $(ORIG_SRC_TAR) --exclude="$(BUILDDIR)/debian" $(BUILDDIR)

.PHONY: deb
deb: $(DEB)
$(DEB) $(DBG_DEB) &: $(BUILDDIR)
	cd $(BUILDDIR); dpkg-buildpackage -b -uc -us
	lintian $(DEB)
	@echo $(DEB)

.PHONY: dsc
dsc:
	rm -rf $(DSC) $(BUILDDIR)
	$(MAKE) $(DSC)
	lintian $(DSC)

$(DSC): $(BUILDDIR) $(ORIG_SRC_TAR)
	cd $(BUILDDIR); dpkg-buildpackage -S -us -uc -d

sbuild: $(DSC)
	sbuild $(DSC)

.PHONY: upload
upload: UPLOAD_DIST ?= $(DEB_DISTRIBUTION)
upload: $(DEB) $(DBG_DEB)
	tar cf - $(DEB) $(DBG_DEB) |ssh -X repoman@repo.proxmox.com -- upload --product pve --dist $(UPLOAD_DIST) --arch $(DEB_HOST_ARCH)

.PHONY: clean distclean
distclean: clean
clean:
	$(CARGO) clean
	rm -rf $(PACKAGE)-[0-9]*/ build/
	rm -f *.deb *.changes *.dsc *.tar.* *.buildinfo *.build .do-cargo-build
	rm -rf tmp_tests
	rm -rf target

.PHONY: dinstall
dinstall: deb
	dpkg -i $(DEB)
