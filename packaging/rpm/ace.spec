%global debug_package %{nil}
%global sname ace

Name:           pgedge-%{sname}
Version:        %{ace_version}
Release:        %{ace_buildnum}%{?dist}
Summary:        The Active Consistency Engine of pgEdge

License:        PostgreSQL License
URL:            https://github.com/pgEdge/%{sname}

Source0:	https://github.com/pgEdge/%{sname}/releases/download/v%{version}/%{sname}_Linux_%{arch}.tar.gz
Source1:        %{name}.service
Source2:        %{name}.tmpfiles.conf
Source3:        %{name}.logrotate
Source4:        NOTICE.txt

BuildRequires:  systemd
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd
Requires:       systemd
Requires:       logrotate

%description
The Active Consistency Engine of pgEdge

%prep
%setup -q -n %{sname}-%{version}
cp %{SOURCE4} .

%build
syft dir:%{_builddir} -o cyclonedx-json > %{_builddir}/%{sname}-sbom.json || exit 1

KEY_ID=$(gpg --list-secret-keys --with-colons | awk -F: '/^sec/{print $5}' | head -n 1); export KEY_ID
gpg --armor --detach-sign --output %{_builddir}/%{sname}-sbom.json.asc %{_builddir}/%{sname}-sbom.json || exit 1

%install
install -D -m 0755 %{sname} %{buildroot}/usr/bin/%{sname}
install -D -m 0644 %{SOURCE1} %{buildroot}%{_unitdir}/%{name}.service
install -D -m 0644 %{SOURCE2} %{buildroot}%{_tmpfilesdir}/%{name}.conf
install -D -m 0644 %{SOURCE3} %{buildroot}%{_sysconfdir}/logrotate.d/%{name}
install -d %{buildroot}/var/lib/pgedge/ace
install -d %{buildroot}/var/log/pgedge/ace
mkdir -p %{buildroot}%{_datadir}/pgedge-%{sname}
install -p -m 0644 %{_builddir}/%{sname}-sbom.json %{buildroot}%{_datadir}/pgedge-%{sname}/%{sname}-sbom.json
install -p -m 0644 %{_builddir}/%{sname}-sbom.json.asc %{buildroot}%{_datadir}/pgedge-%{sname}/%{sname}-sbom.json.asc

%pre
# Ensure pgedge user/group exists
getent group pgedge >/dev/null || groupadd -r pgedge
getent passwd pgedge >/dev/null || \
    useradd -r -g pgedge -d /var/lib/pgedge -s /sbin/nologin \
    -c "pgEdge Services" pgedge
exit 0

%post
%systemd_post %{name}.service
%tmpfiles_create %{_tmpfilesdir}/%{name}.conf

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun_with_restart %{name}.service

%files
%license LICENSE NOTICE.txt
%doc README.md
%{_bindir}/%{sname}
%{_unitdir}/%{name}.service
%{_tmpfilesdir}/%{name}.conf
%config(noreplace) %{_sysconfdir}/logrotate.d/%{name}
%dir %attr(0755,pgedge,pgedge) /var/lib/pgedge
%dir %attr(0755,pgedge,pgedge) /var/lib/pgedge/ace
%dir %attr(0755,pgedge,pgedge) /var/log/pgedge
%dir %attr(0755,pgedge,pgedge) /var/log/pgedge/ace
%{_datadir}/pgedge-%{sname}/%{sname}-sbom.json
%{_datadir}/pgedge-%{sname}/%{sname}-sbom.json.asc

%changelog
* Tue Apr 28 2026 Muhammad Aqeel <muhammad.aqeel@pgedge.com> - 2.0.0
- Initial RPM package of ACE

