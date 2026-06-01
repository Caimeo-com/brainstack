#!/usr/bin/env sh
set -eu

usage() {
  cat <<'USAGE'
Usage: install.sh [--invite-file FILE|-] [--version vX.Y.Z|latest] [--base-url URL] [--bin-dir DIR] [--config FILE] [--skip-enroll] [--skip-init] [--skip-doctor] [--force]

Downloads the platform-specific brainctl binary, verifies its SHA256 sidecar,
installs it to the target bin directory, and optionally runs brainctl enroll.
The checksum catches corrupt downloads; HTTPS or your chosen release host remains
the trust anchor.

If no invite file is provided and a terminal is attached, the installer prompts
for one after brainctl is installed. That keeps token-bearing invites out of
shell history, environment snapshots, and brainctl argv.
USAGE
}

version="${BRAINSTACK_VERSION:-latest}"
base_url="${BRAINSTACK_INSTALL_BASE_URL:-}"
bin_dir="${BRAINSTACK_BIN_DIR:-$HOME/.local/bin}"
invite="${BRAINSTACK_INVITE:-}"
invite_file="${BRAINSTACK_INVITE_FILE:-}"
config_file="${BRAINSTACK_CONFIG:-}"
skip_enroll=0
skip_init=0
skip_doctor=0
force=0
prompt_unavailable=0
allow_unsafe_invite=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --invite)
      [ "$#" -ge 2 ] || { echo "install.sh: --invite requires a value" >&2; exit 2; }
      invite="$2"
      shift 2
      ;;
    --invite=*)
      invite="${1#*=}"
      shift
      ;;
    --invite-file)
      [ "$#" -ge 2 ] || { echo "install.sh: --invite-file requires a value" >&2; exit 2; }
      invite_file="$2"
      shift 2
      ;;
    --invite-file=*)
      invite_file="${1#*=}"
      shift
      ;;
    --version)
      [ "$#" -ge 2 ] || { echo "install.sh: --version requires a value" >&2; exit 2; }
      version="$2"
      shift 2
      ;;
    --version=*)
      version="${1#*=}"
      shift
      ;;
    --base-url)
      [ "$#" -ge 2 ] || { echo "install.sh: --base-url requires a value" >&2; exit 2; }
      base_url="$2"
      shift 2
      ;;
    --base-url=*)
      base_url="${1#*=}"
      shift
      ;;
    --bin-dir)
      [ "$#" -ge 2 ] || { echo "install.sh: --bin-dir requires a value" >&2; exit 2; }
      bin_dir="$2"
      shift 2
      ;;
    --bin-dir=*)
      bin_dir="${1#*=}"
      shift
      ;;
    --config)
      [ "$#" -ge 2 ] || { echo "install.sh: --config requires a value" >&2; exit 2; }
      config_file="$2"
      shift 2
      ;;
    --config=*)
      config_file="${1#*=}"
      shift
      ;;
    --skip-enroll)
      skip_enroll=1
      shift
      ;;
    --skip-init)
      skip_init=1
      shift
      ;;
    --skip-doctor)
      skip_doctor=1
      shift
      ;;
    --force)
      force=1
      shift
      ;;
    --allow-unsafe-invite)
      allow_unsafe_invite=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "install.sh: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ -n "$invite" ] && [ -n "$invite_file" ]; then
  echo "install.sh: use either --invite or --invite-file, not both" >&2
  exit 2
fi

if [ -n "$invite" ] && [ "$allow_unsafe_invite" -ne 1 ]; then
  echo "install.sh: raw invites in argv/env can leak through shell history or process listings; use --invite-file or paste at the prompt" >&2
  echo "install.sh: pass --allow-unsafe-invite only for local throwaway smoke tests" >&2
  exit 2
fi

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$os" in
  darwin) os="darwin" ;;
  linux) os="linux" ;;
  *) echo "install.sh: unsupported OS: $os" >&2; exit 1 ;;
esac
case "$arch" in
  arm64|aarch64) arch="arm64" ;;
  x86_64|amd64) arch="x64" ;;
  *) echo "install.sh: unsupported architecture: $arch" >&2; exit 1 ;;
esac

asset="brainctl-${os}-${arch}"
if [ -z "$base_url" ]; then
  if [ "$version" = "latest" ]; then
    base_url="https://github.com/Caimeo-com/brainstack/releases/latest/download"
  else
    base_url="https://github.com/Caimeo-com/brainstack/releases/download/${version}"
  fi
fi
base_url="${base_url%/}"

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/brainstack-install.XXXXXX")"
saved_stty=""
restore_tty() {
  if [ -n "$saved_stty" ]; then
    stty "$saved_stty" < /dev/tty 2>/dev/null || true
    saved_stty=""
    printf "\n" > /dev/tty 2>/dev/null || true
  fi
}
cleanup() {
  restore_tty
  rm -rf "$tmp_dir"
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

allow_insecure="${BRAINSTACK_INSTALL_ALLOW_INSECURE:-0}"
secure_download_url() {
  url="$1"
  case "$url" in
    https://*) return 0 ;;
    *)
      if [ "$allow_insecure" = "1" ]; then
        return 0
      fi
      echo "install.sh: refusing non-HTTPS download URL: $url" >&2
      echo "set BRAINSTACK_INSTALL_ALLOW_INSECURE=1 only for local release smoke tests" >&2
      return 1
      ;;
  esac
}

download() {
  url="$1"
  out="$2"
  secure_download_url "$url" || return 1
  if command -v curl >/dev/null 2>&1; then
    if [ "$allow_insecure" = "1" ]; then
      curl -fsSL "$url" -o "$out"
    else
      curl -fsSL --proto '=https' --tlsv1.2 "$url" -o "$out"
    fi
  elif command -v wget >/dev/null 2>&1; then
    if [ "$allow_insecure" = "1" ]; then
      wget -q "$url" -O "$out"
    elif wget --help 2>&1 | grep -q -- '--https-only'; then
      wget --https-only --secure-protocol=TLSv1_2 -q "$url" -O "$out"
    else
      echo "install.sh: secure wget download requires --https-only; install curl or set BRAINSTACK_INSTALL_ALLOW_INSECURE=1 for local smoke tests" >&2
      return 1
    fi
  elif command -v fetch >/dev/null 2>&1; then
    if [ "$allow_insecure" = "1" ]; then
      fetch -q -o "$out" "$url"
    else
      echo "install.sh: secure fetch download is not portable; install curl or wget with --https-only" >&2
      return 1
    fi
  else
    echo "install.sh: need curl, wget, or fetch" >&2
    return 1
  fi
}

sha256_file() {
  path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
  elif command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$path" | awk '{print $NF}'
  else
    echo "install.sh: need sha256sum, shasum, or openssl for verification" >&2
    exit 1
  fi
}

binary_path="$tmp_dir/$asset"
checksum_path="$tmp_dir/$asset.sha256"
download "$base_url/$asset" "$binary_path" || { echo "install.sh: failed to download $base_url/$asset" >&2; exit 1; }
download "$base_url/$asset.sha256" "$checksum_path" || { echo "install.sh: failed to download checksum sidecar $base_url/$asset.sha256" >&2; exit 1; }

expected="$(awk '{print $1; exit}' "$checksum_path")"
actual="$(sha256_file "$binary_path")"
if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
  echo "install.sh: checksum mismatch for $asset" >&2
  echo "expected: $expected" >&2
  echo "actual:   $actual" >&2
  exit 1
fi

mkdir -p "$bin_dir"
target="$bin_dir/brainctl"
tmp_target="$bin_dir/.brainctl.$$"
probe="$bin_dir/.brainctl-writable.$$"
if ! ( : > "$probe" ) 2>/dev/null; then
  echo "install.sh: $bin_dir is not writable; rerun with --bin-dir \"\$HOME/.local/bin\" or under sudo" >&2
  exit 1
fi
rm -f "$probe"
cp "$binary_path" "$tmp_target"
chmod 0755 "$tmp_target"
mv "$tmp_target" "$target"
if [ "$os" = "darwin" ] && command -v xattr >/dev/null 2>&1; then
  xattr -d com.apple.quarantine "$target" 2>/dev/null || true
fi

echo "installed brainctl: $target"
case ":$PATH:" in
  *":$bin_dir:"*) ;;
  *) echo "note: add $bin_dir to PATH if brainctl is not found in new shells" ;;
esac

prompt_invite() {
  if [ -n "$invite" ] || [ -n "$invite_file" ] || [ "$skip_enroll" -eq 1 ]; then
    return 0
  fi
  if ( : < /dev/tty ) 2>/dev/null && ( : > /dev/tty ) 2>/dev/null; then
    printf "Brainstack invite (leave blank to skip enroll): " > /dev/tty
    saved_stty="$(stty -g < /dev/tty 2>/dev/null || true)"
    if [ -n "$saved_stty" ]; then
      stty -echo < /dev/tty 2>/dev/null || true
    fi
    IFS= read -r invite < /dev/tty || invite=""
    restore_tty
  else
    prompt_unavailable=1
    echo "install.sh: no terminal available for invite prompt; installed brainctl only. Finish setup with: brainctl enroll --invite-file /path/to/invite.txt" >&2
  fi
}

if [ "$skip_enroll" -eq 0 ]; then
  prompt_invite
fi

if [ "$skip_enroll" -eq 0 ] && { [ -n "$invite" ] || [ -n "$invite_file" ]; }; then
  if [ -n "$invite" ]; then
    invite_tmp="$tmp_dir/invite"
    ( umask 077 && printf '%s\n' "$invite" > "$invite_tmp" )
    invite_file="$invite_tmp"
  fi
  set -- enroll --invite-file "$invite_file"
  if [ -n "$config_file" ]; then
    set -- "$@" --config "$config_file"
  fi
  if [ "$skip_init" -eq 1 ]; then
    set -- "$@" --skip-init
  fi
  if [ "$skip_doctor" -eq 1 ]; then
    set -- "$@" --skip-doctor
  fi
  if [ "$force" -eq 1 ]; then
    set -- "$@" --force
  fi
  BRAINSTACK_INVITE= "$target" "$@"
elif [ "$skip_enroll" -eq 0 ] && [ "$prompt_unavailable" -eq 0 ]; then
  echo "brainctl installed. Create an invite on the control host, then run: brainctl enroll --invite-file /path/to/invite.txt"
fi
