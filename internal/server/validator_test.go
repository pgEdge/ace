package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pgedge/ace/pkg/config"
)

func TestCertValidatorRejectsUnexpectedCN(t *testing.T) {
	caCert, caKey, caPEM := newTestCA(t)
	tempDir := t.TempDir()
	caPath := filepath.Join(tempDir, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	validator, err := newCertValidator(&config.Config{
		CertAuth: config.CertAuthConfig{
			CACertFile: caPath,
		},
		Server: config.ServerConfig{
			AllowedCNs: []string{"alice"},
		},
	})
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	bobCert := newTestClientCert(t, caCert, caKey, "bob", 2)
	if _, err := validator.Validate(bobCert); err == nil {
		t.Fatalf("expected validator to reject client with unexpected CN")
	}

	aliceCert := newTestClientCert(t, caCert, caKey, "alice", 3)
	if _, err := validator.Validate(aliceCert); err != nil {
		t.Fatalf("expected alice certificate to validate, got error: %v", err)
	}
}

func TestCertValidatorRejectsRevokedCertificate(t *testing.T) {
	caCert, caKey, caPEM := newTestCA(t)
	tempDir := t.TempDir()
	caPath := filepath.Join(tempDir, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	aliceCert := newTestClientCert(t, caCert, caKey, "alice", 10)
	crlData := newTestCRL(t, caCert, caKey, aliceCert)
	crlPath := filepath.Join(tempDir, "clients.crl")
	if err := os.WriteFile(crlPath, crlData, 0o600); err != nil {
		t.Fatalf("failed to write CRL file: %v", err)
	}

	validator, err := newCertValidator(&config.Config{
		CertAuth: config.CertAuthConfig{
			CACertFile: caPath,
		},
		Server: config.ServerConfig{
			AllowedCNs:    []string{"alice"},
			ClientCRLFile: crlPath,
		},
	})
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	if _, err := validator.Validate(aliceCert); err == nil {
		t.Fatalf("expected validator to reject revoked certificate")
	}
}

func newTestCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ACE Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("failed to parse CA certificate: %v", err)
	}
	pemData := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return cert, key, pemData
}

func newTestClientCert(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey, cn string, serial int64) *x509.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate client key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject: pkix.Name{
			CommonName: cn,
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create client certificate: %v", err)
	}
	clientCert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("failed to parse client certificate: %v", err)
	}
	return clientCert
}

func newTestCRL(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey, revokedCert *x509.Certificate) []byte {
	t.Helper()
	revoked := []pkix.RevokedCertificate{
		{
			SerialNumber:   revokedCert.SerialNumber,
			RevocationTime: time.Now().Add(-time.Minute),
		},
	}

	crl, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		SignatureAlgorithm:  x509.SHA256WithRSA,
		RevokedCertificates: revoked,
		Number:              big.NewInt(1),
		ThisUpdate:          time.Now().Add(-time.Minute),
		NextUpdate:          time.Now().Add(24 * time.Hour),
	}, caCert, caKey)
	if err != nil {
		t.Fatalf("failed to create CRL: %v", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crl})
}
