cd ~/spool
echo "--- build backend actually used"
uvx -q --from hatchling python -c 'import hatchling.__about__ as a; print("hatchling", a.__version__)'
echo "--- latest twine"
uvx -q --from twine twine --version | head -1
uvx -q --from twine twine check /tmp/sdist/*.whl /tmp/sdist/*.tar.gz
echo "--- pkginfo (what twine uses to parse metadata)"
uvx -q --from pkginfo python -c 'import pkginfo; print("pkginfo", pkginfo.__version__)'
