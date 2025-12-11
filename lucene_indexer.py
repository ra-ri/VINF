import csv
import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import (
    Document, Field, StringField, TextField,
    IntPoint, DoublePoint, StoredField
)
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import MMapDirectory


def index_tv_series(tsv_path, index_dir):
    lucene.initVM()

    analyzer = StandardAnalyzer()
    directory = MMapDirectory(Paths.get(index_dir))
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(directory, config)

    # Fulltext fields
    INDEX_FIELDS = {
        "title",
        "genres", "genre",
        "actors",
        "director",
        "screenwriter",
        "producer",
        "publisher",
        "description",
        "info",
    }

    # Numeric fields
    INT_FIELDS = {"year", "episodes"}
    FLOAT_FIELDS = {"score"}

    # Fields stored only (without indexing)
    STORE_ONLY_FIELDS = {
        "duration",
        "reception",
        "awards"
    }

    with open(tsv_path, newline='', encoding='utf-8') as f:
        # nacita TSV do slovnikov
        reader = csv.DictReader(f, delimiter='\t')
        doc_count = 0

        for row in reader:
            doc = Document()

            doc_id = row.get("doc_id", "").strip()
            if doc_id:
                doc.add(StringField("doc_id", doc_id, Field.Store.YES))

            # Country
            country = row.get("country", "").strip()
            if country:
                doc.add(StringField("country", country, Field.Store.YES))

            # All content for fulltext search - ZAHŔŇA AJ STORE-ONLY POLIA
            all_text_parts = []
            for k, v in row.items():
                if k != "doc_id" and v and v.strip():
                    all_text_parts.append(v.strip())
            
            all_content = " ".join(all_text_parts)
            doc.add(TextField("all_content", all_content, Field.Store.NO))

            # Fulltext fields
            for key in INDEX_FIELDS:
                value = row.get(key, "")
                if value and value.strip():
                    doc.add(TextField(key, value.strip(), Field.Store.YES))

            # Numeric: int
            for key in INT_FIELDS:
                v = row.get(key, "").strip()
                if v.isdigit():
                    num = int(v)
                    doc.add(IntPoint(key, num))
                    doc.add(StoredField(key, num))

            # Numeric: float
            for key in FLOAT_FIELDS:
                v = row.get(key, "").strip()
                try:
                    num = float(v)
                    doc.add(DoublePoint(key, num))
                    doc.add(StoredField(key, num))
                except:
                    pass

            # Store-only fields
            for key in STORE_ONLY_FIELDS:
                value = row.get(key, "")
                if value and value.strip():
                    doc.add(StoredField(key, value.strip()))

            writer.addDocument(doc)
            doc_count += 1

    writer.commit()
    writer.close()
    directory.close()

    print(f"Indexed {doc_count} TV series into '{index_dir}'")


TSV_PATH = "merged.tsv"
INDEX_DIR = "tv_series_index"

index_tv_series(TSV_PATH, INDEX_DIR)
print("Indexing completed.")