package clustercfg

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// genCert writes a PEM cert+key under dir, signed by (ca, caKey) — or self-signed
// when ca is nil — and returns its parsed cert/key + the file paths.
func genCert(t *testing.T, dir, name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, isCA bool) (*x509.Certificate, *ecdsa.PrivateKey, string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}
	if isCA {
		tmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	} else {
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
		tmpl.DNSNames = []string{"localhost", name}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	parent, parentKey := tmpl, key
	if ca != nil {
		parent, parentKey = ca, caKey
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	certPath := filepath.Join(dir, name+".crt")
	keyPath := filepath.Join(dir, name+".key")
	writePEM(t, certPath, "CERTIFICATE", der)
	keyDER, _ := x509.MarshalECPrivateKey(key)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	return cert, key, certPath, keyPath
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		t.Fatal(err)
	}
}

// TestMTLSHandshake checks the Phase 7g TLS helper end to end: a server built
// from ServerConfig (require + verify client cert) accepts a client with a CA-
// signed cert and rejects one with none.
func TestMTLSHandshake(t *testing.T) {
	dir := t.TempDir()
	caCert, caKey, caPath, _ := genCert(t, dir, "ca", nil, nil, true)
	_, _, srvCrt, srvKey := genCert(t, dir, "server", caCert, caKey, false)
	_, _, cliCrt, cliKey := genCert(t, dir, "client", caCert, caKey, false)

	serverTLS, err := (TLS{Cert: srvCrt, Key: srvKey, CA: caPath}).ServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := (TLS{Cert: cliCrt, Key: cliKey, CA: caPath}).ClientConfig("localhost")
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.NextProtos = []string{"http/1.1"} // this test drives the handshake over HTTP/1.1, not h2

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })}
	go srv.Serve(ln)
	defer srv.Close()
	url := "https://" + ln.Addr().String() + "/"

	// mTLS with a CA-signed client cert → the request succeeds.
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS}}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("mTLS request with a client cert failed: %v", err)
	}
	resp.Body.Close()

	// No client cert → the require-and-verify server rejects the connection.
	noCert, err := (TLS{CA: caPath}).ClientConfig("localhost")
	if err != nil {
		t.Fatal(err)
	}
	noCert.NextProtos = []string{"http/1.1"}
	noClient := &http.Client{Transport: &http.Transport{TLSClientConfig: noCert}}
	if resp, err := noClient.Get(url); err == nil {
		resp.Body.Close()
		t.Fatal("server accepted a request with no client cert (mTLS must reject)")
	}
}
