# Releasing

A release ships the **media** variant of `matrix-rtc-ffi` for both mobile
platforms, from this repository:

| Platform | Artifact | Where |
| --- | --- | --- |
| Android | `matrix-rtc-android-<version>.aar` | GitHub Packages Maven repository (always) and Maven Central (when configured); also attached to the GitHub Release |
| iOS | `MatrixRtcFFI.xcframework.zip` | Attached to the GitHub Release; the root `Package.swift` points at it and `Sources/MatrixRtc` holds the matching generated Swift |
| Both | `matrix-rtc-android-<version>-debuginfo.zip`, `SHA256SUMS` | GitHub Release |

The slim (signalling-only) variant is a development build and is not released.

Versions are semver, with one source of truth: `[workspace.package].version` in
the root `Cargo.toml`. Tags are `v<version>`. The Gradle module and
`Package.swift` never carry a hand-edited version.

## Procedure

1. **Release PR.** On a branch:
   - Bump `[workspace.package].version` in `Cargo.toml` and run `cargo check` so
     `Cargo.lock` follows.
   - In `CHANGELOG.md`, rename `## Unreleased` to `## v<version> - <date>` and
     add a fresh empty `## Unreleased` above it. The release workflow refuses
     to run without a section for the version, and uses it as the release
     notes.
   - Merge.

2. **Dry run.** Actions → *Release* → *Run workflow* on the merged branch with
   the version and `dry_run` **checked**. Every job builds; nothing is
   published. Download the `release-v<version>` artifact and sanity-check it:
   the AAR lists `jni/{arm64-v8a,armeabi-v7a,x86_64}/libmatrix_rtc_ffi.so`
   and `libs/libwebrtc.jar`, the xcframework zip has
   `Headers/MatrixRtcFFI/module.modulemap` in each slice, `Package.swift`
   carries the zip's checksum. The POM printed by the "local Maven" step is
   what consumers will resolve.

3. **Release.** Run the workflow again with `dry_run` **unchecked**. It
   publishes the AAR, commits `Package.swift` + `Sources/MatrixRtc` as
   `Release v<version>` on the branch, tags that commit, pushes both, and
   creates the GitHub Release with the assets. The branch must allow the
   `github-actions` bot to push (or dispatch from a branch without protection
   and merge the release commit back).

4. **Announce** the coordinates (see `mobile/PACKAGING.md`, "Consuming a
   release") and, for a breaking release, point integrators at the CHANGELOG's
   *Breaking* section.

## Configuration

Repository secrets read by the workflow:

| Secret | Needed for | Notes |
| --- | --- | --- |
| (none) | GitHub Packages, GitHub Release | Uses the workflow's `GITHUB_TOKEN` |
| `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD` | Maven Central | A Central Portal user token; the namespace (`matrixRtcGroup`, default `io.github.billcarsonfr.matrixrtc`) must be verified on the Portal. Skipped when unset. |
| `SIGNING_KEY_ID`, `SIGNING_KEY`, `SIGNING_PASSWORD` | Maven Central | ASCII-armoured private key (`gpg --armor --export-secret-keys <id>`), its short id and passphrase. Central rejects unsigned artifacts. |

The Maven group is a Gradle property so the artifact can move namespaces
without a code change: pass `-PmatrixRtcGroup=<group>` (edit the `gradlew`
invocations in `.github/workflows/release.yml`, or change the default in
`mobile/android/matrixrtc/build.gradle`).

Consumers of GitHub Packages need a token with `read:packages`; Maven Central
needs none. That is the reason to complete the Central setup.

## Hotfixes

Branch from the tag, fix, bump the patch version, add a CHANGELOG section,
and dispatch the workflow on that branch. The release commit and tag land on
the hotfix branch; merge it forward afterwards.

## Local equivalents

```bash
# What the android jobs run (one target), then what the publish job runs.
MEDIA=1 ./scripts/build-android-aar.sh --target aarch64-linux-android --profile mobile-release --split-debug
MEDIA=1 ./scripts/build-android-aar.sh --skip-build --skip-codegen --version 0.2.0

# What the ios job runs.
MEDIA=1 ./scripts/build-ios-xcframework.sh --profile mobile-release --swift-out Sources/MatrixRtc --zip
./scripts/update-package-swift.sh 0.2.0 "$(cat mobile/ios/build/MatrixRtcFFI.xcframework.zip.sha256)"

# Release notes as the workflow will post them.
./scripts/changelog-section.sh 0.2.0
```

`make release-android` and `make release-ios` run the full-ABI variants of the
first and third commands.
