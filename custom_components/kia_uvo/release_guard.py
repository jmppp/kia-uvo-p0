"""Validate a qualified config-local release before any account authentication."""
import hashlib,json,sys
from pathlib import Path
from homeassistant.const import __version__ as CORE_VERSION
from homeassistant.exceptions import ConfigEntryNotReady

def verify():
    root=Path(__file__).resolve().parent
    release=json.loads((root/'RELEASE.json').read_text())
    if CORE_VERSION!=release['supported_core'] or list(sys.version_info[:2])!=release['python']:
        raise ConfigEntryNotReady('Kia P0 release requires qualification for this Core/Python version')
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.suffix!='.pyc' and p.name!='RELEASE.json'}
    if actual!=set(release['files']):raise ConfigEntryNotReady('Kia P0 release has unexpected/missing files')
    for name,digest in release['files'].items():
        p=root/name
        if not p.resolve().is_relative_to(root) or p.is_symlink() or hashlib.sha256(p.read_bytes()).hexdigest()!=digest:
            raise ConfigEntryNotReady('Kia P0 release integrity check failed')
