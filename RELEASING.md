# Releasing

Releases go to Maven Central through the Central Portal. The `release` workflow builds, tests, signs and
uploads the bundle; a maintainer then confirms the publication in the portal. Nothing is published
automatically.

## One-time setup

- A Central Portal account with the namespace `io.github.peppe-git` verified.
- A GPG key for signing, with its public part on a public key server.
- Repository secrets (Settings → Secrets and variables → Actions):

  | Secret | Content |
  |---|---|
  | `MAVEN_CENTRAL_USERNAME` | user-token username generated in the Central Portal |
  | `MAVEN_CENTRAL_PASSWORD` | user-token password |
  | `GPG_PRIVATE_KEY` | ASCII-armored private key (`gpg --armor --export-secret-keys <key-id>`) |
  | `GPG_PASSPHRASE` | its passphrase |

## Steps

1. **Update the changelog.** Move the entries of `[Unreleased]` in `CHANGELOG.md` under a new
   `[X.Y.Z]` heading with the release date.
2. **Set the release version** and commit:

   ```bash
   ./mvnw -B versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false
   git commit -am "chore(release): X.Y.Z"
   ```

3. **Tag and push:**

   ```bash
   git tag -s vX.Y.Z -m "X.Y.Z"
   git push origin main vX.Y.Z
   ```

   The tag starts `.github/workflows/release.yml`. It refuses to run if the tag does not match the POM
   version or if the version ends in `-SNAPSHOT`. It can also be started by hand on an existing tag
   (Actions → release → Run workflow → pick the tag).
4. **Confirm in the portal.** When the workflow is green, open <https://central.sonatype.com>,
   Deployments: the deployment is `VALIDATED`. Check the list of artifacts (the `compat` module is
   never published), then press **Publish**. Synchronisation to Maven Central takes a few minutes, search
   indexes longer.
5. **Open the next development version:**

   ```bash
   ./mvnw -B versions:set -DnewVersion=X.Y.(Z+1)-SNAPSHOT -DgenerateBackupPoms=false
   git commit -am "chore(release): start X.Y.(Z+1)-SNAPSHOT"
   git push origin main
   ```

6. **Create the GitHub release** from the tag, with the changelog section as notes.

## If something goes wrong

- The workflow failed before the upload: fix, delete and recreate the tag, push it again.
- The deployment failed validation in the portal: drop it there, fix, release a new patch version. A
  version that reached Maven Central can never be changed or deleted.

## Building a release locally

```bash
./mvnw -Prelease -Dgpg.skip -DskipTests verify   # sources and javadoc jars, no signature, no upload
```

`./mvnw -Prelease deploy` from a workstation also works, with a `central` server in
`~/.m2/settings.xml` and `MAVEN_GPG_PASSPHRASE` set, but the workflow is the reference path.
