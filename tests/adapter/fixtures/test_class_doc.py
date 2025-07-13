# tests/fixtures/test_class_doc.py


class TestClassDoc:
    """A test class with full docstrings."""

    def __init__(self, value: int = 0):
        """Initializes the TestClassDoc.

        Args:
            value (int, optional): Initial value. Defaults to 0.
        """
        self._value = value

    def get_value(self) -> int:
        """Gets a value.

        Returns:
            int: The internal value.
        """
        return self._value

    def set_value(self, value: int) -> int:
        """Sets a value.

        Args:
            value (int): The new value.

        Returns:
            int: The updated value.
        """
        self._value = value
        return self._value
