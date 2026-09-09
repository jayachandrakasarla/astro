from some_folder.some_file import some_function

PROBE_MESSAGE = "include.probe_lib imported successfully"


def get_probe_message() -> str:
    some_function()
    return PROBE_MESSAGE
