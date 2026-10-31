_REVIEW_QUEUE_MARK = "def main():"


def _insert_review_queue_section(path: str, section_path: str) -> None:
    text = open(path, encoding="utf-8").read()
    section = open(section_path, encoding="utf-8").read()
    marker = "\ndef main():\n"
    assert text.count(marker) == 1, text.count(marker)
    text = text.replace(marker, "\n" + section.strip("\n") + "\n\n" + marker.lstrip("\n"), 1)
    open(path, "w", encoding="utf-8").write(text)
