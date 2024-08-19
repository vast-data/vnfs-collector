"""
Mocked bcc module for testing purposes.
bcc is external dependency and I don't expect it to be present in all testing environments.
"""

__version__ = "0.0.1"


class BPF:
    def __init__(self, text=None):
        self.text = text

    def load(self, *args, **kwargs):
        pass

    def attach_kprobe(self, *args, **kwargs):
        pass

    def attach_uprobe(self, *args, **kwargs):
        pass

    def detach_kprobe(self, *args, **kwargs):
        pass

    def detach_uprobe(self, *args, **kwargs):
        pass

    def attach_kretprobe(self, *args, **kwargs):
        pass

    def get_kprobe_functions(self, *args, **kwargs):
        pass

    def kernel_struct_has_field(self, *args, **kwargs):
        pass

    def get_table(self, *args, **kwargs):
        pass
