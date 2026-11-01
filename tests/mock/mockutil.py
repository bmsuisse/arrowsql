"""Small helpers shared by the mock tests."""


def tuples(rows):
    """Row objects compare unequal to tuples; normalise for assertions."""
    return [tuple(r) for r in rows]
