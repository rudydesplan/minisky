package main

import (
	"os"
	"strings"
	"testing"
)

func TestPhase20FilestoreTerraformExpectsCanonicalCapacityJSON(t *testing.T) {
	script, err := os.ReadFile("phase20-filestore-terraform-integration.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(script)
	if !strings.Contains(source, `{"name":"minisky","capacityGb":"1024"}`) {
		t.Fatal("Filestore Terraform assertion does not expect string-encoded capacityGb")
	}
	if strings.Contains(source, `{"name":"minisky","capacityGb":1024}`) {
		t.Fatal("Filestore Terraform assertion still accepts non-canonical numeric capacityGb")
	}
}
