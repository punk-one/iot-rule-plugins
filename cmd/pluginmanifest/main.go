package main

import (
	"bufio"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const requiredSDK = "github.com/punk-one/iot-rule-plugin-sdk@v1.0.0"

type manifest struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Version       string `json:"version"`
	Type          string `json:"type"`
	SDK           string `json:"sdk"`
	Module        string `json:"module"`
}

type releaseRecord struct {
	ID        string `json:"id"`
	Version   string `json:"version"`
	Type      string `json:"type"`
	SDK       string `json:"sdk"`
	Module    string `json:"module"`
	Artifact  string `json:"artifact"`
	SHA256    string `json:"sha256"`
	SBOM      string `json:"sbom"`
	Signature string `json:"signature,omitempty"`
}

type sbomDocument struct {
	BOMFormat   string          `json:"bomFormat"`
	SpecVersion string          `json:"specVersion"`
	Version     int             `json:"version"`
	Metadata    sbomMetadata    `json:"metadata"`
	Components  []sbomComponent `json:"components"`
}

type sbomMetadata struct {
	Timestamp string        `json:"timestamp"`
	Component sbomComponent `json:"component"`
}

type sbomComponent struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	PURL    string `json:"purl,omitempty"`
}

type releaseManifest struct {
	SchemaVersion int             `json:"schema_version"`
	CreatedAt     string          `json:"created_at"`
	Commit        string          `json:"commit"`
	GoVersion     string          `json:"go_version"`
	Plugins       []releaseRecord `json:"plugins"`
}

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("usage: pluginmanifest inspect|release ..."))
	}
	var err error
	switch os.Args[1] {
	case "inspect":
		if len(os.Args) != 3 {
			err = errors.New("usage: pluginmanifest inspect <plugin.json>")
		} else {
			err = inspect(os.Args[2])
		}
	case "release":
		if len(os.Args) != 5 {
			err = errors.New("usage: pluginmanifest release <records.tsv> <output.json> <repo-root>")
		} else {
			err = release(os.Args[2], os.Args[3], os.Args[4])
		}
	case "sbom":
		if len(os.Args) != 6 {
			err = errors.New("usage: pluginmanifest sbom <artifact> <output.json> <id> <version>")
		} else {
			err = writeSBOM(os.Args[2], os.Args[3], os.Args[4], os.Args[5])
		}
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fatal(err)
	}
}

func inspect(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var value manifest
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	if err := validate(value); err != nil {
		return fmt.Errorf("validate %s: %w", path, err)
	}
	fmt.Println(value.ID)
	fmt.Println(value.Version)
	fmt.Println(value.Type)
	fmt.Println(value.SDK)
	fmt.Println(value.Module)
	return nil
}

func validate(value manifest) error {
	if value.SchemaVersion != 1 {
		return fmt.Errorf("schema_version=%d, want 1", value.SchemaVersion)
	}
	for name, value := range map[string]string{
		"id": value.ID, "version": value.Version, "type": value.Type,
		"sdk": value.SDK, "module": value.Module,
	} {
		if strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\r\n\t") {
			return fmt.Errorf("%s is empty or contains control characters", name)
		}
	}
	if value.SDK != requiredSDK {
		return fmt.Errorf("sdk=%q, want %q", value.SDK, requiredSDK)
	}
	switch value.Type {
	case "connector", "processor", "sink", "source", "utility":
	default:
		return fmt.Errorf("unsupported plugin type %q", value.Type)
	}
	return nil
}

func release(recordsPath, outputPath, repoRoot string) error {
	input, err := os.Open(recordsPath)
	if err != nil {
		return err
	}
	defer input.Close()

	result := releaseManifest{
		SchemaVersion: 1,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
		Commit:        gitCommit(repoRoot),
		GoVersion:     runtime.Version(),
	}
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) != 9 {
			return fmt.Errorf("invalid release record %q", scanner.Text())
		}
		result.Plugins = append(result.Plugins, releaseRecord{
			ID: fields[0], Version: fields[1], Type: fields[2], SDK: fields[3],
			Module: fields[4], Artifact: fields[5], SHA256: fields[6],
			SBOM: fields[7], Signature: fields[8],
		})
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(result.Plugins) == 0 {
		return errors.New("release contains no plugins")
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(outputPath, encoded, 0o644)
}

func writeSBOM(artifactPath, outputPath, id, version string) error {
	info, err := buildinfo.ReadFile(artifactPath)
	if err != nil {
		return fmt.Errorf("read build info from %s: %w", artifactPath, err)
	}
	document := sbomDocument{
		BOMFormat: "CycloneDX", SpecVersion: "1.5", Version: 1,
		Metadata: sbomMetadata{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Component: sbomComponent{Type: "application", Name: id, Version: version},
		},
		Components: make([]sbomComponent, 0, len(info.Deps)+1),
	}
	document.Components = append(document.Components, moduleComponent(info.Main.Path, info.Main.Version))
	for _, dependency := range info.Deps {
		module := *dependency
		if dependency.Replace != nil {
			module = *dependency.Replace
		}
		document.Components = append(document.Components, moduleComponent(module.Path, module.Version))
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return os.WriteFile(outputPath, encoded, 0o644)
}

func moduleComponent(path, version string) sbomComponent {
	component := sbomComponent{Type: "library", Name: path, Version: version}
	if path != "" && version != "" && version != "(devel)" {
		component.PURL = "pkg:golang/" + path + "@" + version
	}
	return component
}

func gitCommit(repoRoot string) string {
	cmd := exec.Command("git", "-C", repoRoot, "rev-parse", "HEAD")
	output, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "pluginmanifest:", err)
	os.Exit(1)
}
