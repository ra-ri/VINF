import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause, FuzzyQuery, TermQuery, BoostQuery
from org.apache.lucene.document import IntPoint, DoublePoint
from org.apache.lucene.store import MMapDirectory

lucene.initVM()

# Definícia polí, ktoré môžu byť detegované
FIELD_KEYWORDS = {
    'title': 'title',
    'genre': 'genre',
    'author': 'author',
    'country': 'country',
    'director': 'director',
    'network': 'network',
    'cast': 'cast',
    'type': 'type',
    'info': 'info'  # PRIDANÉ
}

NUMERIC_FIELDS = ['year', 'episodes', 'score']
OPERATORS = ['OR', 'AND']


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


# Tokenizácia vstupného dopytu na slová
def tokenize_query(query_input):
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

#Parsuje tokeny a zoskupí ich do štruktúry: [(field, values), (None, values), ...]
def parse_query_tokens(tokens):
    parsed = []
    i = 0

    while i < len(tokens):
        tok = tokens[i]

        # Preskočiť OR/AND (budú sa spracovávať samostatne)
        if tok.upper() in OPERATORS:
            parsed.append(('OPERATOR', tok.upper()))
            i += 1
            continue

        # Kontrola field:value syntax
        if ':' in tok:
            field, value = tok.split(':', 1)
            field = field.strip().lower()
            value = value.strip()
            if value:
                parsed.append((field, [value]))
            i += 1
            continue

        # Kontrola, či je to kľúčové slovo poľa
        tok_lower = tok.lower()
        if tok_lower in FIELD_KEYWORDS:
            field = FIELD_KEYWORDS[tok_lower]
            values = []
            i += 1

            # Zbierať všetky hodnoty až po ďalšie kľúčové slovo alebo operátor
            while i < len(tokens):
                next_tok = tokens[i]
                next_tok_lower = next_tok.lower()

                # Zastaviť, ak je to operátor
                if next_tok.upper() in OPERATORS:
                    break

                # Zastaviť, ak je to ďalšie kľúčové slovo
                if next_tok_lower in FIELD_KEYWORDS:
                    break

                # Zastaviť, ak je to field:value syntax
                if ':' in next_tok:
                    break

                values.append(next_tok)
                i += 1

            if values:
                parsed.append((field, values))
        else:
            # Fulltext token
            parsed.append((None, [tok]))
            i += 1

    return parsed


def search_single_line(index_dir, query_input, top_n=10):
    analyzer = StandardAnalyzer()
    directory = MMapDirectory(Paths.get(index_dir))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)

    builder = BooleanQuery.Builder()

    tokens = tokenize_query(query_input.strip())
    parsed = parse_query_tokens(tokens)

    # Zisti, či je v dopyte OR
    has_or = any(item[0] == 'OPERATOR' and item[1] == 'OR' for item in parsed)
    default_occur = BooleanClause.Occur.SHOULD if has_or else BooleanClause.Occur.MUST

    for item in parsed:
        if item[0] == 'OPERATOR':
            continue

        field, values = item
        value_str = ' '.join(values)

        if field is None:
            # Fulltext vyhľadávanie pre každé slovo
            for val in values:
                fq = FuzzyQuery(Term("all_content", val), 2, 1)
                builder.add(fq, default_occur)

        elif field in NUMERIC_FIELDS:
            # Numerické pole
            min_v, max_v = parse_range(value_str)
            if min_v is None:
                continue
            if field == 'score':
                builder.add(DoublePoint.newRangeQuery(field, min_v, max_v), default_occur)
            else:
                builder.add(IntPoint.newRangeQuery(field, min_v, max_v), default_occur)

        elif field == 'country':
            # Presné vyhľadávanie
            builder.add(TermQuery(Term(field, value_str)), default_occur)

        else:
            # Textové pole s boostovaním
            # Vytvoríme vnútornú BooleanQuery pre každé slovo
            for val in values:
                inner_builder = BooleanQuery.Builder()

                # Primárne hľadanie v detegovanom poli (boost 5.0)
                fq_primary = FuzzyQuery(Term(field, val), 2, 1)
                boosted_primary = BoostQuery(fq_primary, 5.0)
                inner_builder.add(boosted_primary, BooleanClause.Occur.SHOULD)

                # Sekundárne hľadanie v all_content (boost 1.0)
                fq_secondary = FuzzyQuery(Term("all_content", val), 2, 1)
                inner_builder.add(fq_secondary, BooleanClause.Occur.SHOULD)

                builder.add(inner_builder.build(), default_occur)

    query = builder.build()

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

    # Výber dokumentu pre detail
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
    print("\n" + "=" * 60)
    print("Series details:")
    print("=" * 60)
    for field in doc.getFields():
        name = field.name()
        value = field.stringValue()
        if value:
            print(f"{name}: {value}")
    print("\n" + "-" * 60)
    print("Score explanation:")
    print("-" * 60)
    explain = searcher.explain(query, doc_id)
    print(explain.toString())
    print("=" * 60 + "\n")


INDEX_DIR = "tv_series_index"
TOP_N = 10

print("=== Lucene Search ===")
print("\n Query examples:")
print("  vincenzo                           - fulltext search")
print("  title:vincenzo                     - search in title field (exact)")
print("  title vincenzo                     - search primarily in title")
print("  title princess china               - 'princess in title' + 'china' fulltext")
print("  title squid game country south korea - complex multi-field query")
print("  info oscar                         - search in info field")
print("  genre:historical OR genre:drama")
print("  year:2015-2020")
print("  episodes:5-10")
print("  score:7-9")

while True:
    query_input = input("\nQuery: ").strip()
    if not query_input:
        continue
    if query_input.lower() == "exit":
        print("Bye.")
        break

    search_single_line(INDEX_DIR, query_input, top_n=TOP_N)