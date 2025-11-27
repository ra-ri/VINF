import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause, FuzzyQuery, TermQuery
from org.apache.lucene.document import IntPoint, DoublePoint
from org.apache.lucene.store import MMapDirectory

lucene.initVM()

def parse_range(value):
    value = value.strip()
    if not value:
        return None, None
    if '-' in value:
        parts = value.split('-')
        return int(parts[0]), int(parts[1])
    else:
        v = int(value)
        return v, v

def tokenize_query(query_input):
    # Tokenize input by spaces
    
    tokens = []
    current_token = ""
    
    for char in query_input:
        if char == ' ':
            if current_token:
                tokens.append(current_token)
                current_token = ""
        else:
            current_token += char
    
    if current_token:
        tokens.append(current_token)
    
    return tokens

def search_single_line(index_dir, query_input, top_n=10):
    # Fuzzy search in fulltext or in selected field with support for OR/AND.
    analyzer = StandardAnalyzer()
    directory = MMapDirectory(Paths.get(index_dir))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)

    builder = BooleanQuery.Builder()
    numeric_fields = ['year', 'episodes', 'score']
    
    tokens = tokenize_query(query_input.strip())
    
    # Iterate tokens and check if there is any OR operator
    has_or = 'OR' in [t.upper() for t in tokens]
    
    # If there is OR, all conditions must be SHOULD
    # If there is no OR, all are MUST (AND)
    default_occur = BooleanClause.Occur.SHOULD if has_or else BooleanClause.Occur.MUST
    
    i = 0
    
    while i < len(tokens):
        tok = tokens[i]
        # Skip OR/AND tokens
        if tok.upper() in ['OR', 'AND']:
            i += 1
            continue
        
        # Processing field:value or fulltext
        if ':' in tok:
            field, value = tok.split(':', 1)
            field = field.strip().lower()
            value = value.strip()
            if not value:
                i += 1
                continue

            if field in numeric_fields:
                min_v, max_v = parse_range(value)
                if min_v is None:
                    i += 1
                    continue
                if field == 'score':
                    builder.add(DoublePoint.newRangeQuery(field, min_v, max_v), default_occur)
                else:
                    builder.add(IntPoint.newRangeQuery(field, min_v, max_v), default_occur)
            elif field == 'country':
                builder.add(TermQuery(Term(field, value)), default_occur)
            else:
                # FuzzyQuery for text fields
                fq = FuzzyQuery(Term(field, value), 2, 1)
                builder.add(fq, default_occur)
        else:
            # Fulltext fuzzy in all content
            fq = FuzzyQuery(Term("all_content", tok), 2, 1)
            builder.add(fq, default_occur)
        
        i += 1

    query = builder.build()
    
    # DEBUG: Print the created query
    # print(f"\nQuery: '{query_input}'")
    # print(f"Lucene Query: {query}")
    
    hits = searcher.search(query, top_n).scoreDocs

    total_hits = searcher.count(query)
    print(f"Total results found: {total_hits}")
    print(f"Showing TOP {top_n}:\n")

    for i, hit in enumerate(hits, start=1):
        doc = searcher.storedFields().document(hit.doc)
        title = doc.get("title") or "(no title)"
        year_val = doc.get("year") or "N/A"
        doc_id = doc.get("doc_id") or "?"
        print(f"{i}. [{doc_id}] {title} ({year_val}) | score={hit.score:.4f}")

    # Select document for detail view
    select = input("\nEnter number for details, or ENTER for new query: ").strip()
    if select.isdigit():
        idx = int(select) - 1
        if 0 <= idx < len(hits):
            show_document_details(searcher, hits[idx].doc, query)

    reader.close()
    directory.close()
    return hits

def show_document_details(searcher, doc_id, query):
    doc = searcher.storedFields().document(doc_id)
    print("\n" + "="*60)
    print("Series details:")
    print("="*60)
    for field in doc.getFields():
        name = field.name()
        value = field.stringValue()
        if value:
            print(f"{name}: {value}")
    print("\n" + "-"*60)
    print("Score explanation:")
    print("-"*60)
    explain = searcher.explain(query, doc_id)
    print(explain.toString())
    print("="*60 + "\n")

INDEX_DIR = "tv_series_index"
TOP_N = 10

print("=== Lucene  Search ===")
print("Multiple field:value filters")
print("\n Query examples:")
print("  vincenzo")
print("  title:vincenzo")
print("  genre:historical OR genre:drama")
print("  genre:historical AND genre:drama")
print("  year:2015-2020")
print("  episodes:5-10")
print("  score:7-9")

while True:
    query_input = input("Query: ").strip()
    if not query_input:
        continue
    if query_input.lower() == "exit":
        print("Bye.")
        break

    search_single_line(INDEX_DIR, query_input, top_n=TOP_N)