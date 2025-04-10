# CHANGELOG


## v0.2.0-wip.2 (2025-04-10)

### Features

- Testing
  ([`54b7ec4`](https://github.com/ONSdigital/dp-data-pipelines/commit/54b7ec41fdf89561120a5112d1ddf7032e14d403))


## v0.2.0-wip.1 (2025-04-09)

### Bug Fixes

- Attempting to mock timestamp
  ([`06b8d7b`](https://github.com/ONSdigital/dp-data-pipelines/commit/06b8d7b87eaaaca9128d8a49a8667fba688edf75))

- Failing unit test
  ([`c8ff58f`](https://github.com/ONSdigital/dp-data-pipelines/commit/c8ff58fc00f1dc2974835f1c6ea4a1e1a930f1b4))

- Fixing import
  ([`dddd77c`](https://github.com/ONSdigital/dp-data-pipelines/commit/dddd77c0d442ae79fd24c4c806b65e1da967261e))

- Formatting
  ([`27674c0`](https://github.com/ONSdigital/dp-data-pipelines/commit/27674c06de8e2315e12cb627fd38ad3e0c15886c))

- Moving misplaced function
  ([`6f8fda7`](https://github.com/ONSdigital/dp-data-pipelines/commit/6f8fda79a63245587f70ff09fcafe9e0808c5a32))

- Running isort
  ([`6d747fd`](https://github.com/ONSdigital/dp-data-pipelines/commit/6d747fdae0ec616d766441433f78d2531079559e))

- S3_folder_received.start() now handling files as per ticket 2860
  ([`cd542ad`](https://github.com/ONSdigital/dp-data-pipelines/commit/cd542ad425d19c95e0b3eaadfcf0e5c1e6bac14d))

- Sorting more imports
  ([`0ab633d`](https://github.com/ONSdigital/dp-data-pipelines/commit/0ab633d6ba29057329e96de20745f50d935c3acf))

- Test deletes test file
  ([`ea2c7f2`](https://github.com/ONSdigital/dp-data-pipelines/commit/ea2c7f280882b692404a29289eb07d61008da564))

- Tests added for file management functions
  ([`ea44a91`](https://github.com/ONSdigital/dp-data-pipelines/commit/ea44a916f87dcd00c6cdfec6a0ae5639c43b1b1e))

- Tests working
  ([`176504f`](https://github.com/ONSdigital/dp-data-pipelines/commit/176504f4d9d63e21f6ae20d62a11f9bed792982a))

### Chores

- Reduce codeowners to team
  ([`ea99a42`](https://github.com/ONSdigital/dp-data-pipelines/commit/ea99a42e91a07a8736c9e5fc794529ec5f4f7be5))

- Use pyproject.toml for linting + formatting
  ([`aa1287d`](https://github.com/ONSdigital/dp-data-pipelines/commit/aa1287dc8844b4b0b0c80993c08fe2cc8b15af13))

chore: lint

chore: correct versionin makefile

### Continuous Integration

- Add pre-commit config
  ([`1952334`](https://github.com/ONSdigital/dp-data-pipelines/commit/1952334d3c76c22f7b39cf323158eec827f07f47))

chore: add pre-commit to poetry

chore: update poetry lock

- Combine workflows into one, fail on coverage too low
  ([`575f88b`](https://github.com/ONSdigital/dp-data-pipelines/commit/575f88bbdea9c94e218672831da34bed4d459e37))

chore: add permissions

chore: add job names

fix: don't fail immediately

chore: tweak PR comment format

fix: coverage check

fix: output status

fix: handle no code changes

### Documentation

- Adjusting requirements link
  ([`b76e209`](https://github.com/ONSdigital/dp-data-pipelines/commit/b76e2090dfc2a6d3a17e71aaefda25461f86bcb5))

- Remove outdated dataset_ingress docs
  ([`a1abbb3`](https://github.com/ONSdigital/dp-data-pipelines/commit/a1abbb3acb7bfadf9fc457850d470ef89e22f719))

- **conventions**: Add repository convention information
  ([`0065512`](https://github.com/ONSdigital/dp-data-pipelines/commit/0065512e91f6e01a426eb55093a81f21ff48f30b))

- **schema**: Add dummy POST routes for JSON examples
  ([`9ee5e17`](https://github.com/ONSdigital/dp-data-pipelines/commit/9ee5e17c6a13da258b4dfb64c05c32e256e2c159))

- **schema**: Add edition_title + more validation
  ([`f073619`](https://github.com/ONSdigital/dp-data-pipelines/commit/f0736197ec8fa2c997cf6944bc5088f659cd1b9d))

Add the new field `edition_title` inline with the change to the Tier 0 metadata model.

Improve the validation indicators by including minLength for fields that can not be empty,
  specifying that at least one distribution file is needed and indicating that only @ons.gov.uk and
  @ext.ons.gov.uk emails are allowed for the pipeline notifications.

- **schema**: Add examples, descriptions + patterns
  ([`5d65a60`](https://github.com/ONSdigital/dp-data-pipelines/commit/5d65a6033f18e880d797ce337b9ad999ee629baa))

Add examples and descriptions for all properties. Where applicable patterns have been added to
  string fields as well as min and max lengths.

- **schema**: Add OpenAPI specifcation file for manifest + metadata files
  ([`802c979`](https://github.com/ONSdigital/dp-data-pipelines/commit/802c979d8db8048aa0db75e3c8d15e34132e4f7b))

docs(schema): Add OpenAPI specifcation file for manifest + metadata files

docs(schema): Expand README with viewing details

- **schema**: Make edition_title required
  ([`858b997`](https://github.com/ONSdigital/dp-data-pipelines/commit/858b9979a794c9c334182422bf3e9d1d8a72ef3c))

The `edition_title` should have been marked as required.

### Features

- Removed git commit logging
  ([`acb72f9`](https://github.com/ONSdigital/dp-data-pipelines/commit/acb72f99941839d2420b4f32443518af8da97c1e))

Signed-off-by: Moasib-Arif <moasib.arif@ons.gov.uk>

minor changes

chore: remove git package

chore: relock

fix: remove git repo

- Write to temp folder
  ([`bc23bf3`](https://github.com/ONSdigital/dp-data-pipelines/commit/bc23bf3cdb2c0ae7720bb02151e7c558d21da3ef))

fix: f-string

fix: correct object key

wip: write to correct file

fix: remove /temp/ from copy object

fix: correct path (temp -> tmp)

chore: formatting

tests: fix patch matching

chore: linting

- **2708**: Made Request changes for github release workflow
  ([`986cea5`](https://github.com/ONSdigital/dp-data-pipelines/commit/986cea5a0c93d3ec56ea7747ec6e13c50005cc7f))

Updated with python semantics

made requested changes

### Refactoring

- 2649 Refactoring and documentation updates
  ([`f9e2006`](https://github.com/ONSdigital/dp-data-pipelines/commit/f9e200678e58adc6fa07a944d9f1f047fac11921))

- Cleaner utils structure
  ([`c799d01`](https://github.com/ONSdigital/dp-data-pipelines/commit/c799d010b2c10583a186c5026fe329a8b7e528d9))

- Merge utils files
  ([`b917b39`](https://github.com/ONSdigital/dp-data-pipelines/commit/b917b3905f5f339259ee9dd21d8b7c753cbad093))

- Removing shared folder
  ([`473f869`](https://github.com/ONSdigital/dp-data-pipelines/commit/473f86969fb97f04c63fbc0b7c6dca863154a72d))

- **logging**: Remove unneeded logging calls + add explict Exceptions
  ([`c6ed70c`](https://github.com/ONSdigital/dp-data-pipelines/commit/c6ed70c988ea018fe8569c07fab99bfe8ea986f8))

### Testing

- Fix timestamp mock
  ([`6a483aa`](https://github.com/ONSdigital/dp-data-pipelines/commit/6a483aa948e20ddd0984636795e4cb055f5d8ea7))


## v0.1.0 (2024-05-13)
