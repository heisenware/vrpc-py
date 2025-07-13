# tests/fixtures/nested/sample_class_nested.py


class TestClassNested:
    def __init__(self, value=0):
        self._value = value

    def increment(self):
        self._value += 1
        return self._value
