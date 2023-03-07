from invoke import task


@task(aliases=["s"])
def setup(c, version=None, doc=False):
    """
    Setup dev environment, requires conda
    """
    version = version or "3.9"
    suffix = "" if version == "3.9" else version.replace(".", "")
    env_name = f"jupysql{suffix}"

    if doc:
        env_name += "-doc"

    c.run(f"conda create --name {env_name} python={version} --yes")
    c.run(
        'eval "$(conda shell.bash hook)" '
        f"&& conda activate {env_name} "
        "&& pip install --editable .[dev]"
    )

    if doc:
        c.run(
            'eval "$(conda shell.bash hook)" '
            f"&& conda activate {env_name} "
            f"&& conda env update --file doc/environment.yml --name {env_name}"
        )

    print(f"Done! Activate your environment with:\nconda activate {env_name}")


@task(aliases=["d"])
def doc(c):
    with c.cd('doc'):
        c.run(
            "python3 -m sphinx -T -E -W --keep-going -b html \
              -d _build/doctrees -D language=en . _build/html"
        )


@task(aliases=["v"])
def version(c):
    """Create a new stable version commit"""
    from pkgmt import versioneer

    versioneer.version(project_root=".", tag=True)


@task(aliases=["r"])
def release(c, tag, production=True):
    """Upload to PyPI"""
    from pkgmt import versioneer

    versioneer.upload(tag, production=production)
