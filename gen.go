//go:build ignore

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/juicedata/juicefs/pkg/utils"
)

func main() {
	fmt.Println("Gen tls go files")
	pwd, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	tlsPathDir := pwd + "/tls"
	if !utils.Exists(tlsPathDir) {
		fmt.Println("必须将tls文件放在tls目录下")
		os.Exit(-1)
	}
	filepath.Walk(tlsPathDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
			return err
		}
		if info.IsDir() && path != tlsPathDir {
			name := info.Name()
			gofile := pwd + "/pkg/meta/" + name + "-tls.go"
			fmt.Println("Gen go file: ", gofile)
			caCertFile := path + "/ca.crt"
			caData, err := gen_tls_const(name+"_ca_crt", caCertFile)
			if err != nil {
				fmt.Println(err)
				return err
			}
			clientCertFile := path + "/client.crt"
			cilentCertData, err := gen_tls_const(name+"_client_crt", clientCertFile)
			if err != nil {
				fmt.Println(err)
				return err
			}

			clientKeyFile := path + "/client.key"
			clientKeyData, err := gen_tls_const(name+"_client_key", clientKeyFile)
			if err != nil {
				fmt.Println(err)
				return err
			}

			data := fmt.Sprintf("package meta\n\n%s\n\n%s\n\n%s\n", caData, cilentCertData, clientKeyData)

			os.WriteFile(gofile, []byte(data), 0666)
		}
		return nil
	})
}

func gen_tls_const(key, file string) (string, error) {
	if !utils.Exists(file) {
		return "", fmt.Errorf("key file not found")
	}
	fmt.Println("Read file: ", file)
	data, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	out := fmt.Sprintf("const %s = `%s`", key, string(data))
	return out, nil
}
