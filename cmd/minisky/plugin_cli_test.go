package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestGeneratedPluginScaffoldCompiles(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "example")
	if err := writePluginScaffold(target, "example", "example.googleapis.com", "0.1.0"); err != nil {
		t.Fatal(err)
	}
	goMod := "module example.test/plugin\n\ngo 1.26.0\n\nrequire minisky v0.0.0\nreplace minisky => " + root + "\n"
	if err := os.WriteFile(filepath.Join(target, "go.mod"), []byte(goMod), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = target
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated scaffold does not compile: %v\n%s", err, output)
	}
}
