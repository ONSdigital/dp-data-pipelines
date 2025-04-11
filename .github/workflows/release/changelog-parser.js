const fs = require('fs');
const path = require('path'); // Added missing import

const CHANGELOG_PATH = "./CHANGELOG.md";

const getFileAsUTF8 = (filePath) =>  fs.readFileSync(filePath, 'utf8');

const getLatestChangelog = () => {
  const changelog = getFileAsUTF8(CHANGELOG_PATH);
  if (!changelog || typeof changelog !== 'string') {
    throw new Error(`Changelog falsy or not a string; changelog is ${changelog}`);
  }

  const versionSections = changelog.split(/^## v/m);
  const startIndex = versionSections[0].trim().toUpperCase().startsWith("# CHANGELOG") ? 1 : 0;
  
  if (startIndex >= versionSections.length) {
    throw new Error(`Changelog startIndex invalid. startIndex was ${startIndex} and versionSections is ${versionSections}. Changelog: \n\n${changelog}`);
  }
  
  const latestSection = versionSections[startIndex];
  
  return '## ' + latestSection.trim();
} 

module.exports = async () => getLatestChangelog();