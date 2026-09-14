# Preserve the bound IN comparison collation

The legacy IN-to-semi-join rewrite built a bare equality and discarded the
comparison collation already selected for the IN index. A default-BINARY outer
column compared with a NOCASE inner column could then use a NOCASE lookup and
return an extra lowercase row. A projection containing `typeof` and ORDER BY
uses this legacy compilation path. The new logical membership rule already
preserves the bound comparison collation.

The legacy rewrite now attaches that same collation to its left equality
operand. The before-result JSON records the extra row. Three tests cover the
outer column default, an explicit right COLLATE and a computed left operand;
all match SQLite 3.50.4. The correction does not modify automatic-index creation
or the deferred empty-index, UPDATE FROM or nested EXISTS work.
