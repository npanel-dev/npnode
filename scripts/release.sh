#!/usr/bin/env bash
# 发布 NPanel-node 到指定的公开仓库。
#
# 用法：scripts/release.sh <版本号>      例如 scripts/release.sh 1.0.5
#       scripts/release.sh --scripts-only   只同步 install.sh / npnode.sh / README（不编译、不建 Release）
#
# 做的事：
#   1. 编译 linux 64 / arm64-v8a / s390x，打包成 npnode-linux-<arch>.zip
#      （内含 npnode、geoip.dat、geosite.dat、config.yml，与 install.sh 的校验一致）
#   2. 把 scripts/install.sh、scripts/npnode.sh、README.md 同步到发布仓库 main 分支
#   3. 在发布仓库创建 Release <版本号> 并上传安装包和 checksums.txt
#   4. 在源码仓库打同名 tag 并推送
#
# 依赖：go、zip、gh（已登录且对发布仓库有写权限）、git、curl、shasum
set -euo pipefail

RELEASE_REPO="${RELEASE_REPO:-npanel-dev/npnode}"
GEO_BASE="${GEO_BASE:-https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download}"

version="${1:-}"
if [[ -z "$version" ]]; then
    echo "用法: $0 <版本号>，例如 $0 1.0.5" >&2
    exit 1
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

# sync_scripts 把脚本和 README 同步到发布仓库 main 分支
sync_scripts() {
    local msg="$1" dir
    dir="$(mktemp -d)"
    echo "==> 同步脚本到 $RELEASE_REPO"
    git clone -q "https://github.com/$RELEASE_REPO.git" "$dir"
    mkdir -p "$dir/scripts"
    cp scripts/install.sh scripts/npnode.sh "$dir/scripts/"
    cp README.md "$dir/README.md"
    (
        cd "$dir"
        git add -A
        if [[ -n "$(git status --porcelain)" ]]; then
            git commit -q -m "$msg"
            git push -q origin HEAD:main
        else
            echo "    没有变化"
        fi
    )
    rm -rf "$dir"
}

if [[ "$version" == "--scripts-only" ]]; then
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "源码仓库有未提交的修改，请先提交再同步" >&2
        exit 1
    fi
    sync_scripts "scripts: $(git rev-parse --short HEAD)"
    echo "完成"
    exit 0
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "源码仓库有未提交的修改，请先提交再发布" >&2
    exit 1
fi
if gh release view "$version" --repo "$RELEASE_REPO" >/dev/null 2>&1; then
    echo "发布仓库已存在 Release $version" >&2
    exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "==> 下载 geoip.dat / geosite.dat"
curl -fsSL --retry 3 -o "$work/geoip.dat" "$GEO_BASE/geoip.dat"
curl -fsSL --retry 3 -o "$work/geosite.dat" "$GEO_BASE/geosite.dat"

mkdir -p "$work/dist"
# install.sh 里的 arch 名称 → GOARCH
for target in "64:amd64" "arm64-v8a:arm64" "s390x:s390x"; do
    arch="${target%%:*}"
    goarch="${target##*:}"
    echo "==> 编译 linux/$goarch ($arch)"
    pkg="$work/pkg-$arch"
    mkdir -p "$pkg"
    GOEXPERIMENT=jsonv2 GOOS=linux GOARCH="$goarch" CGO_ENABLED=0 \
        go build -trimpath -ldflags "-s -w -buildid= -X github.com/npanel-dev/npnode/cmd.version=$version" \
        -o "$pkg/npnode" .
    cp "$work/geoip.dat" "$work/geosite.dat" config.yml "$pkg/"
    (cd "$pkg" && zip -q -9 "$work/dist/npnode-linux-$arch.zip" npnode geoip.dat geosite.dat config.yml)
done
(cd "$work/dist" && shasum -a 256 *.zip > checksums.txt)

sync_scripts "release: $version"

echo "==> 创建 Release $version"
gh release create "$version" --repo "$RELEASE_REPO" --title "NPanel-node $version" \
    --notes "源码提交：$(git rev-parse --short HEAD)" "$work"/dist/*

echo "==> 源码仓库打 tag $version"
git tag -a "$version" -m "NPanel-node $version"
git push -q origin "$version"

echo "完成：https://github.com/$RELEASE_REPO/releases/tag/$version"
