
class aes():
    """
    Aesthetic mappings

    Parameters
    ----------
    x: str | list
        x aesthetic mapping

    fill : str
        The inner color of a shape

    color : str, default 'None'
        The edge color of a shape
    """

    def __init__(self,
                 x=None,
                 fill=None,
                 color=None):
        self.x = x
        self.fill = fill
        self.color = color
