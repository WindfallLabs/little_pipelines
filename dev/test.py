"""
Development commands / helpers
"""

from subprocess import run

def run_tests() -> None:
    """
    Do pytest stuff
    """
    run(["coverage", "run", "-m", "pytest", "--junitxml=htmlcov/junit.xml"])
    run(["coverage", "report"])
    run(["coverage", "html"])
    run(["coverage", "xml", "-o", "htmlcov/coverage.xml"])

    return


def make_badges() -> None:
    """
    Generate badges
    """
    # We don't need no stinkin' badges!
    run(["genbadge", "tests", "-i", "htmlcov/junit.xml", "-o", "dev/tests-badge.svg"])
    run(["genbadge", "coverage", "-i", "htmlcov/coverage.xml", "-o", "dev/coverage-badge.svg"])

    return


if __name__ == "__main__":
    run_tests()
    make_badges()
