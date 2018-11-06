# Releasing

- do tests
- update translations using ``tx pull -a -af`` (as extra merge request or branch for the case it does not build correctly)
- update the version name and version code of the app
- create a tag/ release in GitHub with an changelog; The tag name should be the version number

## notes

- F-Droid will pick up the new release automatically
- The upload to google play happnes using the CI when a release was tagged
