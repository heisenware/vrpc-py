# tests/adapter/fixtures/sample_class_doc.py


class SampleClassDoc:
    """A sample class with docstrings for testing."""

    def __init__(self, value: int = 0):
        """Constructor

        :param value: Initial value. Defaults to 0.
        :type value: int
        """
        self._value = value

    def get_value(self) -> int:
        """Gets a value

        :returns: the internal value
        """
        return self._value

    def set_value(self, value: int) -> int:
        """Sets a value

        :param value: The new value
        :returns: the updated value
        :rtype: int
        """
        self._value = value
        return self._value
