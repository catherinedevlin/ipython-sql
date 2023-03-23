from abc import abstractmethod


class geom:
    """
    Base class of all geom
    """

    def __init__(self):
        pass

    def __add__(self, gg):
        return gg

    def __radd__(self, gg):
        return gg + self

    @abstractmethod
    def draw(self, gg):
        """
        Draws plot
        """
        pass
