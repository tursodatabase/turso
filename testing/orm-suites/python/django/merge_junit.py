"""merge_junit.py <out.xml> <in.xml>...: concatenate the test suites of JUnit files.

An input that is empty or unparsable (its test app failed before running any
test) is skipped; run.sh records why in <driver>-setup-errors.txt.
"""

import sys
import xml.etree.ElementTree as ET

merged = ET.Element("testsuites")
for path in sys.argv[2:]:
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        continue
    merged.extend(root.iter("testsuite"))
ET.ElementTree(merged).write(sys.argv[1], encoding="utf-8", xml_declaration=True)
