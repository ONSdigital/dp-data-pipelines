const fs = require('fs');
const path = require('path'); // Added missing import
const getLatestChangelog = require('./release/changelog-parser');

const getFileAsUTF8 = (filePath) =>  fs.readFileSync(filePath, 'utf8');

class GitHubReleaseManager {
  constructor(github, context) {
    this.github = github;
    this.owner = context.repo.owner;
    this.repo = context.repo.repo;
    this.branch = process.env.BRANCH;
    this.tag = process.env.TAG;
    this.isPrelease = process.env.IS_PRERELEASE ?? false;
  }

  async createBlobForFile(filePath) {
    const content = getFileAsUTF8(filePath);
    const blobData = await this.github.rest.git.createBlob({
      owner: this.owner,
      repo: this.repo,
      content,
      encoding: 'utf-8',
    });
    return blobData.data;
  }

  async createNewTree(blobs, paths, parentTreeSha) {
    const tree = blobs.map(({ sha }, index) => ({
      path: paths[index],
      mode: '100644',
      type: 'blob',
      sha,
    }));
    
    const { data } = await this.github.rest.git.createTree({
      owner: this.owner,
      repo: this.repo,
      tree,
      base_tree: parentTreeSha,
    });
    
    return data;
  }

  async createNewCommit(message, treeSha, parentCommitSha) {
    const { data } = await this.github.rest.git.createCommit({
      owner: this.owner,
      repo: this.repo,
      message,
      tree: treeSha,
      parents: [parentCommitSha],
    });
    
    return data;
  }

  async setBranchToCommit(branch, commitSha) {
    return this.github.rest.git.updateRef({
      owner: this.owner,
      repo: this.repo,
      ref: `heads/${branch}`,
      sha: commitSha,
    });
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
    const releaseNotes = getLatestChangelog({});

    return this.github.rest.repos.createRelease({
      owner: this.owner,
      repo: this.repo,
      tag_name: tag,
      name: `Release ${tag}`,
      body: releaseNotes,
      draft: false,
      prerelease: false,
      generate_release_notes: false
    });
  }

  async createNewRelease(coursePath = '') {
    const latestCommitSha = await this.getLatestCommitHash();
    
    const { data: commitData } = await this.github.rest.git.getCommit({
      owner: this.owner,
      repo: this.repo,
      commit_sha: latestCommitSha,
    });
    
    const currentCommitTreeSha = commitData.tree.sha;
    
    const filesPaths = ["./pyproject.toml"];
    
    const filesBlobs = await Promise.all(
      filesPaths.map(filePath => this.createBlobForFile(filePath))
    );
    
    const pathsForBlobs = filesPaths.map(
      fullPath => path.relative(coursePath, fullPath)
    );
    
    const newTree = await this.createNewTree(
      filesBlobs,
      pathsForBlobs,
      currentCommitTreeSha
    );
    
    const newCommit = await this.createNewCommit(
      `chore: Release ${this.tag}`,
      newTree.sha,
      latestCommitSha
    );
    
    await this.setBranchToCommit(this.branch, newCommit.sha);
    
    const tagObj = await this.createNewTag(this.tag, latestCommitSha);
    
    await this.createRefForTag(this.tag, tagObj);
    
    await this.createRelease(this.tag);
  }
}

module.exports = async ({ github, context, core }) => {
  const releaseManager = new GitHubReleaseManager(github, context);
  await releaseManager.createNewRelease();
};