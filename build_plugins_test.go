package plugins_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
)

func TestBuildPluginsScriptBuildsExecutablePluginAndLatestSymlink(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("plugin artifact assertions are Linux-specific")
	}

	repoRoot := buildScriptFixtureRepo(t)
	scriptPath := filepath.Join(repoRoot, "build-plugins.sh")

	cmd := exec.Command("bash", scriptPath)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"GOWORK=off",
		"GOCACHE="+filepath.Join(t.TempDir(), "go-build-cache"),
		"PLUGINS_OVERRIDE=sources/source_mqtt",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run build-plugins.sh: %v\n%s", err, output)
	}

	latestPath := filepath.Join(repoRoot, "dist", "plugins", "sources", "source-mqtt-latest.so")
	latestInfo, err := os.Lstat(latestPath)
	if err != nil {
		t.Fatalf("stat latest artifact: %v\n%s", err, output)
	}
	if latestInfo.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected %s to be a symlink, mode=%v", latestPath, latestInfo.Mode())
	}

	runCmd := exec.Command(latestPath)
	runOutput, runErr := runCmd.CombinedOutput()
	if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				t.Fatalf("expected executable plugin artifact, got signal %v\n%s", status.Signal(), runOutput)
			}
		}
	}
	if !bytes.Contains(runOutput, []byte("fixture plugin")) {
		t.Fatalf("unexpected plugin executable output: %q", runOutput)
	}

	releaseData, err := os.ReadFile(filepath.Join(repoRoot, "dist", "plugins", "release-manifest.json"))
	if err != nil {
		t.Fatalf("read release manifest: %v", err)
	}
	if !bytes.Contains(releaseData, []byte(`"id": "source-mqtt"`)) ||
		!bytes.Contains(releaseData, []byte(`"sha256":`)) {
		t.Fatalf("release manifest missing identity or checksum: %s", releaseData)
	}
}

func buildScriptFixtureRepo(t *testing.T) string {
	t.Helper()

	repoRoot := t.TempDir()
	sourceRoot := repoSourceRoot(t)

	mustCopyFile(t,
		filepath.Join(sourceRoot, "build-plugins.sh"),
		filepath.Join(repoRoot, "build-plugins.sh"),
		0o755,
	)
	mustCopyFile(t,
		filepath.Join(sourceRoot, "cmd", "pluginmanifest", "main.go"),
		filepath.Join(repoRoot, "cmd", "pluginmanifest", "main.go"),
		0o644,
	)

	mustWriteFile(t, filepath.Join(repoRoot, "go.mod"), "module fixture-build-script\n\ngo 1.25.0\n", 0o644)
	mustWriteFile(t, filepath.Join(repoRoot, "sources", "source_mqtt", "go.mod"), "module fixture-source-mqtt\n\ngo 1.25.0\n", 0o644)
	mustWriteFile(t, filepath.Join(repoRoot, "sources", "source_mqtt", "plugin.json"), `{"schema_version":1,"id":"source-mqtt","version":"1.2.3","type":"source","sdk":"github.com/punk-one/iot-rule-plugin-sdk@v1.0.0","module":"fixture-source-mqtt"}`+"\n", 0o644)
	mustWriteFile(t, filepath.Join(repoRoot, "sources", "source_mqtt", "main.go"), "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"fixture plugin\")\n}\n", 0o644)
	mustWriteFile(t, filepath.Join(repoRoot, "sources", "source_mqtt", "plugin.go"), "package main\n\nvar _ = struct {\n\tID      string\n\tVersion string\n}{\n\tID:      \"source-mqtt\",\n\tVersion: \"1.2.3\",\n}\n", 0o644)

	return repoRoot
}

func repoSourceRoot(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	return filepath.Dir(thisFile)
}

func mustCopyFile(t *testing.T, srcPath, dstPath string, mode os.FileMode) {
	t.Helper()

	if err := mustCopyFileErr(srcPath, dstPath, mode); err != nil {
		t.Fatalf("copy file %s -> %s: %v", srcPath, dstPath, err)
	}
}

func mustWriteFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func mustCopyFileErr(srcPath, dstPath string, mode os.FileMode) error {
	input, err := os.ReadFile(srcPath)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(dstPath, input, mode)
}
