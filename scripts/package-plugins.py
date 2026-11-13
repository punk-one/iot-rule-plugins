#!/usr/bin/env python3
"""Make independently installable archives; never include directory/symlink entries."""
import argparse
import hashlib
import json
import pathlib
import subprocess
import tarfile
import tempfile

p = argparse.ArgumentParser()
p.add_argument("--root", default="dist/plugins")
p.add_argument("--output", default="dist/releases")
p.add_argument("--sign", action="store_true")
args = p.parse_args()
root, output = pathlib.Path(args.root), pathlib.Path(args.output)
output.mkdir(parents=True, exist_ok=True)
manifest = json.loads((root / "release-manifest.json").read_text())
for record in manifest["plugins"]:
    record["sbom_sha256"] = hashlib.sha256((root / record["sbom"]).read_bytes()).hexdigest()
    os_name, arch = record.get("os", "linux"), record.get("arch", "amd64")
    name = f'{record["id"]}-v{record["version"]}-{os_name}-{arch}'
    with tempfile.TemporaryDirectory() as temp:
        mini = pathlib.Path(temp) / "release-manifest.json"
        mini.write_text(json.dumps(dict(manifest, plugins=[record]), indent=2) + "\n")
        if args.sign:
            subprocess.run(["cosign", "sign-blob", "--yes", "--bundle", str(mini) + ".sigstore.json", str(mini)], check=True)
        archive = output / (name + ".tar.gz")
        with tarfile.open(archive, "w:gz") as tar:
            tar.add(mini, arcname="plugins/release-manifest.json", recursive=False)
            if args.sign:
                tar.add(str(mini) + ".sigstore.json", arcname="plugins/release-manifest.json.sigstore.json", recursive=False)
            for field in ("artifact", "sbom", "signature"):
                if record.get(field):
                    tar.add(root / record[field], arcname="plugins/" + record[field], recursive=False)
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        (output / (archive.name + ".sha256")).write_text(f"{digest}  {archive.name}\n")
        print(archive)
