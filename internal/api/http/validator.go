package server

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pgedge/ace/pkg/config"
)

type certValidator struct {
	clientCAPool   *x509.CertPool
	allowedCNs     map[string]struct{}
	revokedSerials map[string]struct{}
	crl            *pkix.CertificateList
	nextUpdate     time.Time
}

func newCertValidator(cfg *config.Config) (*certValidator, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration not provided")
	}

	caPath := strings.TrimSpace(cfg.CertAuth.CACertFile)
	if caPath == "" {
		return nil, fmt.Errorf("cert_auth.ca_cert_file must be configured")
	}

	caPool, caCerts, err := loadCACerts(caPath)
	if err != nil {
		return nil, err
	}

	allowedCNs := make(map[string]struct{})
	for _, cn := range cfg.Server.AllowedCNs {
		if trimmed := strings.TrimSpace(cn); trimmed != "" {
			allowedCNs[trimmed] = struct{}{}
		}
	}

	validator := &certValidator{
		clientCAPool:   caPool,
		allowedCNs:     allowedCNs,
		revokedSerials: make(map[string]struct{}),
	}

	clientCRL := strings.TrimSpace(cfg.Server.ClientCRLFile)
	if clientCRL != "" {
		crl, revoked, nextUpdate, err := loadCRL(clientCRL, caCerts)
		if err != nil {
			return nil, err
		}
		validator.crl = crl
		validator.revokedSerials = revoked
		validator.nextUpdate = nextUpdate
	}

	return validator, nil
}

func (v *certValidator) Validate(cert *x509.Certificate) (string, error) {
	if cert == nil {
		return "", fmt.Errorf("client certificate is missing")
	}
	now := time.Now()
	if now.Before(cert.NotBefore) {
		return "", fmt.Errorf("client certificate not valid before %s", cert.NotBefore.Format(time.RFC3339))
	}
	if now.After(cert.NotAfter) {
		return "", fmt.Errorf("client certificate expired at %s", cert.NotAfter.Format(time.RFC3339))
	}

	commonName := strings.TrimSpace(cert.Subject.CommonName)
	if commonName == "" {
		return "", fmt.Errorf("client certificate is missing a common name (CN)")
	}

	if len(v.allowedCNs) > 0 {
		if _, ok := v.allowedCNs[commonName]; !ok {
			return "", fmt.Errorf("client certificate CN %q is not allowed", commonName)
		}
	}

	if v.crl != nil {
		if !v.nextUpdate.IsZero() && now.After(v.nextUpdate) {
			return "", fmt.Errorf("client CRL expired at %s", v.nextUpdate.Format(time.RFC3339))
		}
		serial := cert.SerialNumber.String()
		if _, revoked := v.revokedSerials[serial]; revoked {
			return "", fmt.Errorf("client certificate with serial %s has been revoked", serial)
		}
	}

	return commonName, nil
}

func loadCACerts(path string) (*x509.CertPool, []*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read client CA file: %w", err)
	}

	pool := x509.NewCertPool()
	var certs []*x509.Certificate
	remaining := data

	for len(remaining) > 0 {
		block, rest := pem.Decode(remaining)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			remaining = rest
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse CA certificate: %w", err)
		}
		pool.AddCert(cert)
		certs = append(certs, cert)
		remaining = rest
	}

	if len(certs) == 0 {
		cert, err := x509.ParseCertificate(data)
		if err != nil {
			return nil, nil, fmt.Errorf("no CA certificates found in %s", path)
		}
		pool.AddCert(cert)
		certs = append(certs, cert)
	}

	return pool, certs, nil
}

func loadCRL(path string, caCerts []*x509.Certificate) (*pkix.CertificateList, map[string]struct{}, time.Time, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("failed to read client CRL file: %w", err)
	}

	var crlBytes []byte
	if block, _ := pem.Decode(data); block != nil {
		if block.Type != "X509 CRL" {
			return nil, nil, time.Time{}, fmt.Errorf("expected X509 CRL block, found %s", block.Type)
		}
		crlBytes = block.Bytes
	} else {
		crlBytes = data
	}

	crl, err := x509.ParseCRL(crlBytes)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("failed to parse CRL: %w", err)
	}

	if len(caCerts) == 0 {
		return nil, nil, time.Time{}, fmt.Errorf("cannot verify CRL without CA certificates")
	}

	var verified bool
	for _, ca := range caCerts {
		if err := ca.CheckCRLSignature(crl); err == nil {
			verified = true
			break
		}
	}
	if !verified {
		return nil, nil, time.Time{}, fmt.Errorf("unable to verify CRL signature with configured CA certificates")
	}

	revoked := make(map[string]struct{})
	for _, revokedCert := range crl.TBSCertList.RevokedCertificates {
		if revokedCert.SerialNumber == nil {
			continue
		}
		revoked[revokedCert.SerialNumber.String()] = struct{}{}
	}

	return crl, revoked, crl.TBSCertList.NextUpdate, nil
}
