package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestUsageDoesNotAdvertiseServe(t *testing.T) {
	var buf bytes.Buffer
	orig := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	usage()
	_ = w.Close()
	os.Stderr = orig
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read usage: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "kilroy serve") {
		t.Fatalf("usage still advertises serve:\n%s", out)
	}
}

func TestServeCommandIsRemoved(t *testing.T) {
	bin := buildKilroyBinary(t)
	code, out := runKilroy(t, bin, "serve")
	if code == 0 {
		t.Fatalf("kilroy serve exited 0, expected removed command failure\n%s", out)
	}
	if strings.Contains(out, "listening on") {
		t.Fatalf("kilroy serve started a server instead of failing:\n%s", out)
	}
}
