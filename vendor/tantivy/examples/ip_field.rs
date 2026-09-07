// # IP Address example
//
// This example shows how the ip field can be used
// with IpV6 and IpV4.

use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, INDEXED, STORED, STRING};
use tantivy::{Index, IndexWriter, TantivyDocument};

fn main() -> tantivy::Result<()> {
    // # Defining the schema
    // We set the IP field as `INDEXED`, so it can be searched
    // `FAST` will create a fast field. The fast field will be used to execute search queries.
    // `FAST` is not a requirement for range queries, it can also be executed on the inverted index
    // which is created by `INDEXED`.
    let mut schema_builder = Schema::builder();
    let event_type = schema_builder.add_text_field("event_type", STRING | STORED);
    let ip = schema_builder.add_ip_addr_field("ip", STORED | INDEXED | FAST);
    let schema = schema_builder.build();

    // # Indexing documents
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // ### IPv4
    // Adding documents that contain an IPv4 address. Notice that the IP addresses are passed as
    // `String`. Since the field is of type ip, we parse the IP address from the string and store it
    // internally as IPv6.
    let doc = TantivyDocument::parse_json(
        &schema,
        r#"{
            "ip": "192.168.0.33",
            "event_type": "login"
        }"#,
    )?;
    index_writer.add_document(doc)?;
    let doc = TantivyDocument::parse_json(
        &schema,
        r#"{
            "ip": "192.168.0.80",
            "event_type": "checkout"
        }"#,
    )?;
    index_writer.add_document(doc)?;
    // ### IPv6
    // Adding a document that contains an IPv6 address.
    let doc = TantivyDocument::parse_json(
        &schema,
        r#"{
            "ip": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            "event_type": "checkout"
        }"#,
    )?;

    index_writer.add_document(doc)?;
    // Commit will create a segment containing our documents.
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // # Search
    // Range queries on IPv4. Since we created a fast field, the fast field will be used to execute
    // the search.
    // ### Range Queries
    let query_parser = QueryParser::for_index(&index, vec![event_type, ip]);
    {
        // Inclusive range queries
        let query = query_parser.parse_query("ip:[192.168.0.80 TO 192.168.0.100]")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(5).order_by_score())?;
        assert_eq!(count_docs.len(), 1);
    }
    {
        // Exclusive range queries
        let query = query_parser.parse_query("ip:{192.168.0.80 TO 192.168.1.100]")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
        assert_eq!(count_docs.len(), 0);
    }
    {
        // Find docs with IP addresses smaller equal 192.168.1.100
        let query = query_parser.parse_query("ip:[* TO 192.168.1.100]")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
        assert_eq!(count_docs.len(), 2);
    }
    {
        // Find docs with IP addresses smaller than 192.168.1.100
        let query = query_parser.parse_query("ip:[* TO 192.168.1.100}")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
        assert_eq!(count_docs.len(), 2);
    }

    // ### Exact Queries
    // Exact search on IPv4.
    {
        let query = query_parser.parse_query("ip:192.168.0.80")?;
        let count_docs = searcher.search(&*query, &Count)?;
        assert_eq!(count_docs, 1);
    }
    // Exact search on IPv6.
    // IpV6 addresses need to be quoted because they contain `:`
    {
        let query = query_parser.parse_query("ip:\"2001:0db8:85a3:0000:0000:8a2e:0370:7334\"")?;
        let count_docs = searcher.search(&*query, &Count)?;
        assert_eq!(count_docs, 1);
    }

    Ok(())
}
