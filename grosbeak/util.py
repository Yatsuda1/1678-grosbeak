from os import listdir
from os.path import isfile, join


def all_files_in_dir(dir: str) -> list[str]:
    """
    This function returns all the files in a directory
    It uses the listdir and isfile functions from the os module
    to get all the files in the directory
    """
    return [f for f in listdir(dir) if isfile(join(dir, f))]


def strip_extension(filename: str) -> str:
    """
    This function strips the extension from a filename.
    For example, it would return "hello" if you called
    it with "hello.txt".
    """
    return filename.split(".")[0]


def serialize_documents(docs: list[dict]) -> list[dict]:
    """
    Remove the '_id' from each document and
    returns the documents in the same order
    """
    docs = list(docs)
    for doc in docs:
        doc.pop("_id", None)

    return docs
