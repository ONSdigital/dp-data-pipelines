const { release } = require('os');
const getChangelog = require('./changelog-parser');

//Remove the first line (with the version number header) and empty lines
const removeVersionNumberLine = (changelog) => changelog.split("\n").slice(2).join("\n").trim();

const getReleaseNotes = async () => {
  const releaseNotes = await getChangelog();

  return !releaseNotes.startsWith("## ") ? releaseNotes : removeVersionNumberLine(releaseNotes);
}

class GitHubReleaseManager {
  constructor(github, context) {
    this.github = github;
    this.owner = context.repo.owner;
    this.repo = context.repo.repo;
    this.branch = process.env.BRANCH;
    this.tag = process.env.TAG;
    this.isPrelease = process.env.IS_PRERELEASE ?? false;
  }

  async getLatestCommitHash() {
    const { data: refData } = await this.github.rest.git.getRef({
      owner: this.owner,
      repo: this.repo,
      ref: `heads/${this.branch}`
    });
    
    return refData.object.sha;
  }

  async createNewTag(tag, commitSha) {
    const { data } = await this.github.rest.git.createTag({
      owner: this.owner,
      repo: this.repo,
      tag,
      message: `Release ${tag}`,
      object: commitSha,
      type: 'commit',
      tagger: {
        name: 'github-actions[bot]',
        email: 'github-actions[bot]@users.noreply.github.com'
      }
    });
    
    return data;
  }

  async createRefForTag(tag, tagObj) {
    return this.github.rest.git.createRef({
      owner: this.owner,
      repo: this.repo,
      ref: `refs/tags/${tag}`,
      sha: tagObj.sha
    });
  }

  async createRelease(tag) {
    const releaseNotes = await getReleaseNotes();

    return this.github.rest.repos.createRelease({
      owner: this.owner,
      repo: this.repo,
      tag_name: tag,
      name: tag,
      body: releaseNotes,
      draft: false,
      prerelease: this.isPrelease,
      generate_release_notes: false
    });
  }

  async createNewRelease() {
    const latestCommitSha = await this.getLatestCommitHash();
    
    const tagObj = await this.createNewTag(this.tag, latestCommitSha);
    
    await this.createRefForTag(this.tag, tagObj);
    
    await this.createRelease(this.tag);
  }
}

module.exports = async ({ github, context, core }) => {
  const releaseManager = new GitHubReleaseManager(github, context);
  await releaseManager.createNewRelease();
};