# Json

As of tantivy 0.17, tantivy supports a json object type.
This type can be used to allow for a schema-less search index.

When indexing a json object, we "flatten" the JSON. This operation emits terms that represent a triplet `(json_path, value_type, value)`

For instance,  if user is a json field, the following document:

```json
{
    "user": {
        "name": "Paul Masurel",
        "address": {
            "city": "Tokyo",
            "country": "Japan"
        },
        "created_at": "2018-11-12T23:20:50.52Z"
    }
}
```

emits the following tokens:

- ("name", Text, "Paul")
- ("name", Text, "Masurel")
- ("address.city", Text, "Tokyo")
- ("address.country", Text, "Japan")
- ("created_at", Date, 15420648505)

## Bytes-encoding and lexicographical sort

Like any other terms, these triplets are encoded into a binary format as follows.

- `json_path`: the json path is a sequence of "segments". In the example above, `address.city`
is just a debug representation of the json path `["address", "city"]`.
Its representation is done by separating segments by a unicode char `\x01`, and ending the path by `\x00`.
- `value type`: One byte represents the `Value` type.
- `value`: The value representation is just the regular Value representation.

This representation is designed to align the natural sort of Terms with the lexicographical sort
of their binary representation (tantivy's dictionary (whether fst or sstable) is sorted and does prefix encoding).

In the example above, the terms will be sorted as

- ("address.city", Text, "Tokyo")
- ("address.country", Text, "Japan")
- ("name", Text, "Masurel")
- ("name", Text, "Paul")
- ("created_at", Date, 15420648505)

As seen in "pitfalls", we may end up having to search for a value for a same path in several different fields. Putting the field code after the path makes it maximizes compression opportunities but also increases the chances for the two terms to end up in the actual same term dictionary block.

## Pitfalls, limitation and corner cases

Json gives very little information about the type of the literals it stores.
All numeric types end up mapped as a "Number" and there are no types for dates.

At indexing, tantivy will try to interpret number and strings as different type with a
priority order.

Numbers will be interpreted as u64, i64 and f64 in that order.
Strings will be interpreted as rfc3339 dates or simple strings.

The first working type is picked and is the only term that is emitted for indexing.
Note this interpretation happens on a per-document basis, and there is no effort to try to sniff
a consistent field type at the scale of a segment.

On the query parser side on the other hand, we may end up emitting more than one type.
For instance, we do not even know if the type is a number or string based.

So the query

```rust
my_path.my_segment:233
```

Will be interpreted as

```rust
(my_path.my_segment, String, 233) or (my_path.my_segment, u64, 233)
```

Likewise, we need to emit two tokens if the query contains an rfc3339 date.
Indeed the date could have been actually a single token inside the text of a document at ingestion time. Generally speaking, we will always at least emit a string token in query parsing, and sometimes more.

If one more json field is defined, things get even more complicated.

## Default json field

If the schema contains a text field called "text" and a json field that is set as a default field:
`text:hello` could be reasonably interpreted as targeting the text field or as targeting the json field called `json_dynamic` with the json_path "text".

If there is such an ambiguity, we decide to only search in the "text" field: `text:hello`.

In other words, the parser will not search in default json fields if there is a schema hit.
This is a product decision.

The user can still target the JSON field by specifying its name explicitly:
`json_dynamic.text:hello`.

## Range queries are not supported

Json field do not support range queries.

## Arrays do not work like nested object

If json object contains an array, a search query might return more documents
than what might be expected.

Let's take an example.

```json
{
    "cart_id": 3234234 ,
    "cart": [
        {"product_type": "sneakers", "attributes": {"color": "white"} },
        {"product_type": "t-shirt", "attributes": {"color": "red"}},
    ]
}
```

Despite the array structure, a document in tantivy is a bag of terms.
The query:

```rust
cart.product_type:sneakers AND cart.attributes.color:red
```

Actually match the document above.
