
REPO=$1
WHEEL=$2

twine upload --repository-url $REPO $WHEEL
