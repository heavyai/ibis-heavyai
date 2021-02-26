$PROJECT = 'ibis-omniscidb'
$ACTIVITIES = [
    'tag',  # Creates a tag for the new version number
    'push_tag',  # Pushes the tag up to the $TAG_REMOTE
    'ghrelease'  # Creates a Github release entry for the new tag
]

# Repo to push tags to
$PUSH_TAG_REMOTE = 'git@github.com:xmnlab/ibis-omniscidb.git'
# $TAG_TEMPLATE = 'v$VERSION'

$GITHUB_ORG = 'xmnlab'  # Github org for Github releases and conda-forge
$GITHUB_REPO = 'ibis-omniscidb'  # Github repo for Github releases  and conda-forge
