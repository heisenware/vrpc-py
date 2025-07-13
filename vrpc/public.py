# vrpc/public.py

class Vrpc:
    """Provides the @Vrpc.public decorator."""

    @staticmethod
    def public(func):
        """
        A decorator to mark a method as publicly callable via VRPC.
        """
        func._is_vrpc_public = True
        return func
