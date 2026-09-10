import requests

LANGUAGES = [
    "danish",
    "dutch",
    "finnish",
    "french",
    "german",
    "hungarian",
    "italian",
    "norwegian",
    "portuguese",
    "russian",
    "spanish",
    "swedish",
]

with requests.Session() as sess, open("stopwords.rs", "w") as mod:
    mod.write("/*\n")
    mod.write(
        "These stop word lists are from the Snowball project (https://snowballstem.org/)\nwhich carries the following copyright and license:\n\n"
    )

    resp = sess.get(
        "https://raw.githubusercontent.com/snowballstem/snowball/master/COPYING"
    )
    resp.raise_for_status()
    mod.write(resp.text)
    mod.write("*/\n\n")

    for lang in LANGUAGES:
        resp = sess.get(f"https://snowballstem.org/algorithms/{lang}/stop.txt")
        resp.raise_for_status()

        mod.write(f"pub const {lang.upper()}: &[&str] = &[\n")

        for line in resp.text.splitlines():
            line, _, _ = line.partition("|")

            for word in line.split():
                mod.write(f'    "{word}",\n')

        mod.write("];\n\n")
