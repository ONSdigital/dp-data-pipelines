# CHANGELOG


## v0.2.0-rc.2 (2025-05-14)

### Bug Fixes

- Made and required fields in models
  ([`2b92cef`](https://github.com/ONSdigital/dp-data-pipelines/commit/2b92cefcba850bea54a5e2d70a58804b3b10b482))

- **dataset**: Make release_date a str
  ([`f20294a`](https://github.com/ONSdigital/dp-data-pipelines/commit/f20294aaaee40545f4c438de65e593cb076b78fb))

chore: linting

### Documentation

- Update pipeline requirements
  ([`8bed45b`](https://github.com/ONSdigital/dp-data-pipelines/commit/8bed45bbacd3926373501bdf31b16d010984efd5))

chore: update readmes

chore: add code notes


## v0.2.0-rc.1 (2025-04-29)

### Bug Fixes

- Add xls mimetype
  ([`ce73418`](https://github.com/ONSdigital/dp-data-pipelines/commit/ce734187bef7ed3f8bfee8ff489903a5c4b724a4))

chore: utils linting

- Addressed PR comments
  ([`6a778b9`](https://github.com/ONSdigital/dp-data-pipelines/commit/6a778b92da7c6e85b2e284bf22d00a6d960b3a47))

- All tests passing
  ([`366a165`](https://github.com/ONSdigital/dp-data-pipelines/commit/366a165a2918af0c88a1576648f0a747e4f6d6af))

- Attempting to mock timestamp
  ([`06b8d7b`](https://github.com/ONSdigital/dp-data-pipelines/commit/06b8d7b87eaaaca9128d8a49a8667fba688edf75))

- Correct mimetype for csdb to match dataset API definition
  ([`82105e9`](https://github.com/ONSdigital/dp-data-pipelines/commit/82105e91d2ff6a97177408f7de8ddc08430bc680))

- Failing unit test
  ([`c8ff58f`](https://github.com/ONSdigital/dp-data-pipelines/commit/c8ff58fc00f1dc2974835f1c6ea4a1e1a930f1b4))

- Fixing import
  ([`dddd77c`](https://github.com/ONSdigital/dp-data-pipelines/commit/dddd77c0d442ae79fd24c4c806b65e1da967261e))

- Formatting
  ([`27674c0`](https://github.com/ONSdigital/dp-data-pipelines/commit/27674c06de8e2315e12cb627fd38ad3e0c15886c))

- Initialise notifier + email client as None
  ([`a906011`](https://github.com/ONSdigital/dp-data-pipelines/commit/a906011848e43b95b2be60713a1175a94ce73595))

- Mock_job_config missing after rebase
  ([`a595d66`](https://github.com/ONSdigital/dp-data-pipelines/commit/a595d663f356a53a6112297f3bdf7bd2c9f04f0c))

- Moving misplaced function
  ([`6f8fda7`](https://github.com/ONSdigital/dp-data-pipelines/commit/6f8fda79a63245587f70ff09fcafe9e0808c5a32))

- Nearly all tests passing
  ([`88c4059`](https://github.com/ONSdigital/dp-data-pipelines/commit/88c405938190790320a01c74c0d743741c5ba88d))

- Poetry lock
  ([`11be0f2`](https://github.com/ONSdigital/dp-data-pipelines/commit/11be0f2b6511b1a75ee7741abe22e2f256c5063e))

- Pretty sure i already did
  ([`3de9208`](https://github.com/ONSdigital/dp-data-pipelines/commit/3de92082aedcc72419f2e68cbf3ca52a43de3f7a))

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

- Remove BDD tests
  ([`954dbab`](https://github.com/ONSdigital/dp-data-pipelines/commit/954dbab376610895d804085de100f6f151132b44))

Not being updated, only causing problems currently. Not needed; will be covered by integration tests
  soon.

fix: dependency lock

- Simplify disabled notifications check
  ([`68962e3`](https://github.com/ONSdigital/dp-data-pipelines/commit/68962e379e0ece26a3d848418a6b997105ec7122))

- Simplify emails disabled
  ([`de23ca4`](https://github.com/ONSdigital/dp-data-pipelines/commit/de23ca4280ae80d1f1a4fa207a6aa4093d0dc439))

- Update dp-python-tools to v0.7.0
  ([`e15cf65`](https://github.com/ONSdigital/dp-data-pipelines/commit/e15cf659beec35dc58d63cb21f7cbb54bfce9cfb))

- Update Python packages
  ([`380b99c`](https://github.com/ONSdigital/dp-data-pipelines/commit/380b99c4710f5470140475244d00dd1b2d7078f5))

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

- Verified commits + release
  ([`99a3be5`](https://github.com/ONSdigital/dp-data-pipelines/commit/99a3be5de1b01a586c0d045f8ea4734a10ac50ac))

fix: cache

chore: amend cache further

- **tests**: Amend coverage to be greater than or equal to
  ([`403412a`](https://github.com/ONSdigital/dp-data-pipelines/commit/403412a1a449ffda0cd82beff7615ee1a5eb2761))

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

- Add moto (S3, SES, Secrets Manager) and pytest-mock to dev dependencies
  ([`769351d`](https://github.com/ONSdigital/dp-data-pipelines/commit/769351d8a1e757deb7317b8514599c6bb27021c8))

- Add validator for sqlite db files
  ([`9c3591e`](https://github.com/ONSdigital/dp-data-pipelines/commit/9c3591eecf0b613274423208b7e7aa635c8c8ffe))

sqlite formatting

chore sqlite validator remove testing

- Function added to check if dataset type is static
  ([`c3b8b92`](https://github.com/ONSdigital/dp-data-pipelines/commit/c3b8b9293e0edb5365e5893393dbb0ce04e7d54d))

feat: oops

feat: Comments addressed

fix: make lint

fix: Removed test that needs rewriting

fix: Remove finally block as this was not right

fix: comments addressed

- Improve CSVValidator
  ([`fe29355`](https://github.com/ONSdigital/dp-data-pipelines/commit/fe293554078f098091814bfbf958adf4af5cbf1b))

csv validator fmt lint

fix

- Removed git commit logging
  ([`acb72f9`](https://github.com/ONSdigital/dp-data-pipelines/commit/acb72f99941839d2420b4f32443518af8da97c1e))

Signed-off-by: Moasib-Arif <moasib.arif@ons.gov.uk>

minor changes

chore: remove git package

chore: relock

fix: remove git repo

- String formatting for ValidationResult
  ([`a0e1e61`](https://github.com/ONSdigital/dp-data-pipelines/commit/a0e1e61333a858d52eb7499f3ceb22d17c9308b4))

chore: formatting models

tests: validate model string

- Update dpytools version to v0.8.0
  ([`f47bed5`](https://github.com/ONSdigital/dp-data-pipelines/commit/f47bed51875a386c4c0681a28393b7c58687e77e))

- Validate file matches the file format of its extension
  ([`678a9d5`](https://github.com/ONSdigital/dp-data-pipelines/commit/678a9d5ea7523b4ea09906a9cb6cc7968d6ded75))

Fix the xls tests

improved test coverage

Minor changes

feat: refactor file validation

update references

fix imports

wip fixing tests

chore: add openpyxl for pandas processing of excel files

chore: linting

tests: update validation unit tests

chore: formatting

tests: test validate_file_format

- Validate file matches the file format of its extension
  ([`ddef652`](https://github.com/ONSdigital/dp-data-pipelines/commit/ddef652830d98b4e36433201a31dae09031cacc0))

Fix the xls tests

improved test coverage

Minor changes

feat: refactor file validation

update references

fix imports

wip fixing tests

chore: add openpyxl for pandas processing of excel files

chore: linting

tests: update validation unit tests

chore: formatting

tests: test validate_file_format

- Wip using dataclasses
  ([`ac633dc`](https://github.com/ONSdigital/dp-data-pipelines/commit/ac633dcdb98cb31480d26b60174207889ada5c78))

fix: updating test case files

fix: most tests passing

fix: removed unnecessary functions

fix: tests passing

fix: moved test case files

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

- **config**: Add class for loading configuration
  ([`a64052f`](https://github.com/ONSdigital/dp-data-pipelines/commit/a64052f8768f1fb45078e7ebfe253ee757c6b881))

tests: Add tests for job config

chore: linting + formatting

chore: Add docs string comment

chore: linting

chore: Update package ver

feat: WIP secrets config class and tests

feat: Working tests and notifier change

feat: Adding docstrings

fix: Formatting fixes

fix: removing leftover

fix: Fixing jobconfiguration instances

troubleshooting

Fixing unit tests - Co-authored-by: jimwashbrook-ons

fix: More formatting

fix: Import orders

fix: Lint and more imports

fix: Unit tests passing

fix: S3 unit test mocking

fix: More formatting fixes

fix: Unused variable

feat: Refactor JobConfiguration secrets definitions

wip: refactor

fix: tests

feat: add logging

chore: export env vars

linting formatting

chore: delete thing

chore: formatting

- **config**: Raise exception in JobConfiguration if errored
  ([`81a8c35`](https://github.com/ONSdigital/dp-data-pipelines/commit/81a8c35ce233cf8658b22cfb7b34e63d383c5010))

- **errors**: Raise original exception
  ([`4383502`](https://github.com/ONSdigital/dp-data-pipelines/commit/4383502bb758b0b0758db2810ba00a55eaa5ec72))

chore: pass existing notifier to error handler

fix: raise err again

### Refactoring

- 2649 Refactoring and documentation updates
  ([`f9e2006`](https://github.com/ONSdigital/dp-data-pipelines/commit/f9e200678e58adc6fa07a944d9f1f047fac11921))

- Amend default configs to methods to allow mocking
  ([`6b32263`](https://github.com/ONSdigital/dp-data-pipelines/commit/6b3226385daeba5b2dfba4bf19ade7d94e675061))

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
