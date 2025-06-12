//go:build ignore

package main

import (
	"fmt"
	"os"
	"strings"
	"github.com/juicedata/juicefs/pkg/utils"
)

func genTlsConst(key, file string) (string, error) {
	if !utils.Exists(file) {
		return "", fmt.Errorf("key file not found")
	}

	fmt.Println("Reading file: ", file)
	data, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("const %s = `%s`", key, string(data)), nil
}

func main() {
	fmt.Println("Gen tls go files for metadata engines, config file ./tls/{tikv,etcd,...}/<crt files>")
	pwd, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	tlsParentDir := pwd + "/tls"
	if !utils.Exists(tlsParentDir) {
		fmt.Println("tls dir %s not found", tlsParentDir)
		os.Exit(-1)
	}

	metadaEngines := []string{"tikv", "etcd"}
	for _, engine := range metadaEngines {
		certDir := tlsParentDir + "/" + engine

		if !utils.Exists(certDir) {
			fmt.Printf("Skip generating for engine %s as tls dir not found\n", engine)
			continue
		}

		fmt.Printf("Generating certificate files for metadata engine: %s\n", engine)

		gofile := pwd + "/pkg/meta/" + engine + "-tls.go"
		fmt.Println("Generating go file: ", gofile)

		caData, err := genTlsConst(engine+"_ca_crt", certDir+"/ca.crt")
		if err != nil {
			fmt.Printf("Generate ca data failed for engine %v: %v\n", engine, err)
			os.Exit(-2)
		}

		cilentCertData, err := genTlsConst(engine+"_client_crt", certDir+"/client.crt")
		if err != nil {
			fmt.Printf("Generate client cert data failed for engine %v: %v\n", engine, err)
			os.Exit(-2)
		}

		clientKeyData, err := genTlsConst(engine+"_client_key", certDir+"/client.key")
		if err != nil {
			fmt.Printf("Generate client key data failed for engine %v: %v\n", engine, err)
			os.Exit(-2)
		}

		engineUpper := strings.ToUpper(engine)
		getFunction := fmt.Sprintf("func Get%sTlsData() (string) {\n\treturn %s\n}\n", engineUpper+"Ca", "tikv_ca_crt")
		getFunction += fmt.Sprintf("func Get%sTlsData() (string) {\n\treturn %s\n}\n", engineUpper+"ClientCert", "tikv_client_crt")
		getFunction += fmt.Sprintf("func Get%sTlsData() (string) {\n\treturn %s\n}\n", engineUpper+"ClientKey", "tikv_client_key")

		data := fmt.Sprintf("package meta\n\n%s\n\n%s\n\n%s\n\n%s\n", caData, cilentCertData, clientKeyData, getFunction)

		os.WriteFile(gofile, []byte(data), 0666)
		fmt.Printf("Generate tls go files for metadata engine %s done\n", engine)
	}

	fmt.Printf("Generate tls done\n")
}
