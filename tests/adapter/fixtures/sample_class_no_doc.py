# tests/fixtures/sample_class_no_doc.py


class SampleClassNoDoc:
    def __init__(self, value=0):
        self._value = value

    def get_value(self):
        return self._value
