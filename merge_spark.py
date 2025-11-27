from pyspark import SparkContext, SparkConf
import re

# Spark context
conf = SparkConf().setAppName("MergeTSVFiles")
sc = SparkContext(conf=conf)

# Cesty k súborom
mydramalist_path = r"C:\Users\JLR\Documents\FIIT_ING\VINF2\P1\extracted.tsv"
wiki_path = r"C:\Users\JLR\Documents\FIIT_ING\VINF2\tv_series_20251123215600.tsv"
output_path = r"C:\Users\JLR\Documents\FIIT_ING\VINF2\merged.tsv"


def normalize_year(year_str):
    if not year_str or year_str.strip() == "\t":
        return ""

    # Odstránenie špeciálnych znakov
    cleaned = re.sub(r"[^\d]", " ", year_str)

    # Nájdenie prvého 4-ciferného čísla
    match = re.search(r"\b(\d{4})\b", cleaned)
    if match:
        return match.group(1)

    return ""


def parse_tsv_line(line, is_header=False):
    parts = line.split("\t")
    return parts


def create_join_key(title, year):
    """
    Vytvorí kľúč pre join: (normalizovaný title, normalizovaný rok)
    """
    # Vtvorenie kľúča (normalizovaný title, normalizovaný rok)
    norm_title = title.strip().lower() if title else ""
    norm_year = normalize_year(year)
    return (norm_title, norm_year)


# Načítanie MDL súboru
mdl_lines = sc.textFile(mydramalist_path)
mdl_header_line = mdl_lines.first()
mdl_header = parse_tsv_line(mdl_header_line)
mdl_data = mdl_lines.filter(lambda line: line != mdl_header_line)

# Parsovanie MDL dát
mdl_parsed = mdl_data.map(lambda line: parse_tsv_line(line))


# Vytvorenie MDL párov (key, dict)
def mdl_to_pair(parts):
    if len(parts) < len(mdl_header):
        parts += [""] * (len(mdl_header) - len(parts))
    row_dict = {mdl_header[i]: parts[i] for i in range(len(mdl_header))}
    title = row_dict.get("title", "")
    year = row_dict.get("year", "")
    key = create_join_key(title, year)
    return (key, ("mdl", row_dict))


mdl_pairs = mdl_parsed.map(mdl_to_pair)

# Načítanie Wiki súboru
wiki_lines = sc.textFile(wiki_path)
wiki_header_line = wiki_lines.first()
wiki_header = parse_tsv_line(wiki_header_line)
wiki_data = wiki_lines.filter(lambda line: line != wiki_header_line)

# Parsovanie Wiki dát
wiki_parsed = wiki_data.map(lambda line: parse_tsv_line(line))


# Vytvorenie Wiki párov (key, dict)
def wiki_to_pair(parts):
    if len(parts) < len(wiki_header):
        parts += [""] * (len(wiki_header) - len(parts))
    row_dict = {wiki_header[i]: parts[i] for i in range(len(wiki_header))}
    title = row_dict.get("title", "")
    year = row_dict.get("year", "")
    key = create_join_key(title, year)
    return (key, ("wiki", row_dict))


wiki_pairs = wiki_parsed.map(wiki_to_pair)

# Full outer join
all_pairs = mdl_pairs.union(wiki_pairs)
grouped = all_pairs.groupByKey()

# Spojenie všetkých stĺpcov
all_columns = ["doc_id"]
seen_cols = {"doc_id"}

for col in mdl_header:
    if col not in seen_cols:
        all_columns.append(col)
        seen_cols.add(col)

# Pridanie country ak chýba
if "country" not in seen_cols:
    all_columns.insert(1, "country")
    seen_cols.add("country")

for col in wiki_header:
    if col not in seen_cols:
        all_columns.append(col)
        seen_cols.add(col)


def merge_group(key_values_pair):
    """
    Spája MDL a Wiki riadky podľa kľúča.
    """
    key, values = key_values_pair
    values_list = list(values)

    mdl_rows = [v[1] for v in values_list if v[0] == "mdl"]
    wiki_rows = [v[1] for v in values_list if v[0] == "wiki"]

    results = []

    # Spracovanie MDL riadkov
    for mdl_row in mdl_rows:
        merged = {c: "" for c in all_columns}

        # Naplnenie MDL údajmi
        for col, val in mdl_row.items():
            if col in merged:
                merged[col] = val.strip()

        # Doplnenie Wiki údajmi (ak existujú)
        if wiki_rows:
            wiki_row = wiki_rows[0]  # Použijeme prvý match
            for col, val in wiki_row.items():
                if col in merged and not merged[col]:
                    merged[col] = val.strip()

        # Zabezpečenie country
        if not merged.get("country"):
            merged["country"] = ""

        results.append(merged)

    # Pridanie nových Wiki riadkov (ktoré nemajú MDL match)
    if not mdl_rows:
        for wiki_row in wiki_rows:
            merged = {c: "" for c in all_columns}
            for col, val in wiki_row.items():
                if col in merged:
                    merged[col] = val.strip()
            if not merged.get("country"):
                merged["country"] = ""
            results.append(merged)

    return results


# Spracovanie a spojenie
merged_rdd = grouped.flatMap(merge_group)

# Pridanie doc_id
merged_with_id = merged_rdd.zipWithIndex().map(lambda x: {**x[0], "doc_id": str(x[1])})

# Konverzia na TSV formát
def dict_to_tsv(row_dict):
    return "\t".join([row_dict.get(col, "") for col in all_columns])

output_lines = merged_with_id.map(dict_to_tsv)

# Vytvorenie hlavičky
header_line = "\t".join(all_columns)

# Uloženie do dočasného súboru
temp_output = output_path + "_temp"
output_lines.saveAsTextFile(temp_output)

# Zlúčenie častí do jedného súboru
import glob
import shutil
import os

part_files = sorted(glob.glob(os.path.join(temp_output, "part-*")))

if part_files:
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(header_line + "\n")
        for part_file in part_files:
            with open(part_file, "r", encoding="utf-8") as pf:
                content = pf.read()
                if content.strip():
                    f.write(content)
                    if not content.endswith("\n"):
                        f.write("\n")

    # Vymazanie dočasných súborov
    shutil.rmtree(temp_output)

    # Štatistiky
    total_count = merged_with_id.count()
    mdl_count = mdl_pairs.count()
    wiki_count = wiki_pairs.count()

    # Spočítanie joinov
    joined_keys = mdl_pairs.join(wiki_pairs).count()

    print(f"Done! File saved as: {output_path}")
    print(f"Final number of series: {total_count}")
    print(f"Original MDL series: {mdl_count}")
    print(f"Original Wiki series: {wiki_count}")
    print(f"Joined series: {joined_keys}")
    print(f"New series from wiki: {total_count - mdl_count}")
else:
    print("No data to merge")

sc.stop()