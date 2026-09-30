package plugins_test

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type pluginManifest struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Version       string `json:"version"`
	Type          string `json:"type"`
	SDK           string `json:"sdk"`
	Module        string `json:"module"`
}

func TestPluginsAreIndependentModulesAndManifestsMatch(t *testing.T) {
	root := repoSourceRoot(t)
	manifestPaths := make([]string, 0, 23)
	for _, category := range []string{"connectors", "processors", "sinks", "sources"} {
		matches, err := filepath.Glob(filepath.Join(root, category, "*", "plugin.json"))
		if err != nil {
			t.Fatal(err)
		}
		manifestPaths = append(manifestPaths, matches...)
	}
	if len(manifestPaths) != 23 {
		t.Fatalf("plugin manifests=%d, want 23", len(manifestPaths))
	}

	seen := make(map[string]string, len(manifestPaths))
	for _, manifestPath := range manifestPaths {
		pluginDir := filepath.Dir(manifestPath)
		manifest := readManifest(t, manifestPath)
		if manifest.SchemaVersion != 1 || manifest.SDK != "github.com/punk-one/iot-rule-plugin-sdk@v1.0.0" {
			t.Fatalf("invalid frozen fields in %s: %+v", manifestPath, manifest)
		}
		if previous := seen[manifest.ID]; previous != "" {
			t.Fatalf("duplicate plugin id %q in %s and %s", manifest.ID, previous, manifestPath)
		}
		seen[manifest.ID] = manifestPath

		if modulePath := goModulePath(t, pluginDir); modulePath != manifest.Module {
			t.Fatalf("%s module=%q, manifest=%q", pluginDir, modulePath, manifest.Module)
		}
		info := sourcePluginInfo(t, filepath.Join(pluginDir, "plugin.go"))
		if info["ID"] != manifest.ID || info["Version"] != manifest.Version || info["Type"] != manifest.Type {
			t.Fatalf("%s source identity=%v, manifest=%+v", pluginDir, info, manifest)
		}
		assertNoCrossPluginImports(t, pluginDir)

		mainData, err := os.ReadFile(filepath.Join(pluginDir, "main.go"))
		if err != nil {
			t.Fatal(err)
		}
		entrypoint := "sdk.ServeFactory"
		if manifest.Type == "connector" {
			entrypoint = "sdk.ServeConnector"
		}
		if !strings.Contains(string(mainData), entrypoint) {
			t.Fatalf("%s must use %s", pluginDir, entrypoint)
		}
	}
}

func readManifest(t *testing.T, path string) pluginManifest {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result pluginManifest
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return result
}

func goModulePath(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Path}}")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -m in %s: %v\n%s", dir, err, output)
	}
	return strings.TrimSpace(string(output))
}

func sourcePluginInfo(t *testing.T, path string) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]string{}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		selector, ok := literal.Type.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "PluginInfo" {
			return true
		}
		for _, element := range literal.Elts {
			field, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			name, ok := field.Key.(*ast.Ident)
			if !ok {
				continue
			}
			switch value := field.Value.(type) {
			case *ast.BasicLit:
				decoded, _ := strconv.Unquote(value.Value)
				result[name.Name] = decoded
			case *ast.SelectorExpr:
				if name.Name == "Type" {
					result[name.Name] = strings.ToLower(strings.TrimPrefix(value.Sel.Name, "PluginType"))
				}
			}
		}
		return false
	})
	if result["ID"] == "" {
		t.Fatalf("PluginInfo not found in %s", path)
	}
	return result
}

func assertNoCrossPluginImports(t *testing.T, pluginDir string) {
	t.Helper()
	entries, err := filepath.Glob(filepath.Join(pluginDir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range entries {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range file.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(value, "github.com/punk-one/iot-rule-plugins/") {
				t.Fatalf("%s imports another plugin module %s", path, value)
			}
		}
	}
}
