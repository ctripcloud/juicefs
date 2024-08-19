package utils

import "os"

func CreateCertFile(ca, cert, key []byte) (string, string, string, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", "", "", err
	}
	tlsPathDir := dir + "/.trip_juicefs"
	cacertFile := tlsPathDir + "/juice_client.roo"
	certFile := tlsPathDir + "/juice_client.pub"
	keyFile := tlsPathDir + "/juice_client.pri"
	if Exists(cacertFile) && Exists(certFile) && Exists(keyFile) {
		return cacertFile, certFile, keyFile, nil
	}
	if err := os.MkdirAll(tlsPathDir, 0666); err != nil {
		return "", "", "", err
	}
	perm := os.FileMode(0666)
	if err := os.WriteFile(cacertFile, []byte(ca), perm); err != nil {
		return "", "", "", err
	}
	if err := os.WriteFile(certFile, []byte(cert), perm); err != nil {
		return "", "", "", err
	}
	if err := os.WriteFile(keyFile, []byte(key), perm); err != nil {
		return "", "", "", err
	}
	return cacertFile, certFile, keyFile, nil
}
